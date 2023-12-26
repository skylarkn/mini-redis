use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// 从远程对等体发送和接收 `Frame` 值。
///
/// 在实现网络协议时，协议上的消息通常由几个称为帧的较小消息组成。`Connection` 的目的是在底层的 `TcpStream` 上读取和写入帧。
///
/// 要读取帧，`Connection` 使用内部缓冲区，该缓冲区被填充直到有足够的字节来创建完整的帧为止。一旦发生这种情况，`Connection` 就会创建帧并将其返回给调用方。
///
/// 当发送帧时，帧首先被编码到写缓冲区中。然后将写缓冲区的内容写入套接字。
#[derive(Debug)]
pub struct Connection {
    // `TcpStream`。它装饰了一个 `BufWriter`，提供写级别的缓冲。Tokio 提供的 `BufWriter` 实现对我们的需求已经足够了。
    stream: BufWriter<TcpStream>,

    // 用于读取帧的缓冲区。
    buffer: BytesMut,
}

impl Connection {
    /// 创建一个新的 `Connection`，由 `socket` 支持。初始化读取和写入缓冲区。
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // 默认使用 4KB 读取缓冲区。对于 mini redis 的用例，这是可以的。但是，真实的应用程序将希望调整此值以适应其特定的用例。很可能更大的读取缓冲区效果更好。
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// 从底层流中读取单个 `Frame` 值。
    ///
    /// 该函数等待直到它已检索足够的数据来解析一帧。在解析帧之后，保留在读缓冲区中的任何剩余数据将保留在那里，供下一次调用 `read_frame` 使用。
    ///
    /// # 返回
    ///
    /// 成功时，返回接收到的帧。如果 `TcpStream` 以不会将帧分隔开的方式关闭，则返回 `None`。否则，返回错误。
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // 尝试从缓冲数据中解析一个帧。如果缓冲的数据足够，就会返回帧。
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // 缓冲区中没有足够的数据来读取帧。尝试从套接字中读取更多数据。
            //
            // 成功时，返回读取的字节数。`0` 表示“流结束”。
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // 远程关闭了连接。为了使其成为正常关闭，读缓冲区中不应有数据。如果有，这意味着对等体在发送帧时关闭了套接字。
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("对等方重置了连接".into());
                }
            }
        }
    }

    /// 尝试从缓冲区解析帧。如果缓冲区包含足够的数据，则返回帧并从缓冲区中删除数据。如果尚未缓冲足够的数据，则返回 `Ok(None)`。如果缓冲的数据不表示有效的帧，则返回 `Err`。
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // 使用 Cursor 跟踪缓冲区中的“当前”位置。Cursor 还实现了来自 `bytes` crate 的 `Buf`，为使用字节提供了许多有用的工具。
        let mut buf = Cursor::new(&self.buffer[..]);

        // 首先检查是否已经缓冲足够的数据来解析单个帧。这一步通常比对帧的完整解析要快得多，并且允许我们跳过分配数据结构以保存帧数据，除非我们知道已接收到完整的帧。
        match Frame::check(&mut buf) {
            Ok(_) => {
                // `check` 函数将使光标前进到帧的末尾。由于在调用 `Frame::check` 之前，光标的位置被设置为零，因此通过检查光标位置来获取帧的长度。
                let len = buf.position() as usize;

                // 在传递光标给 `Frame::parse` 之前，将位置重置为零。
                buf.set_position(0);

                // 从缓冲区解析帧。这将分配用于表示帧的必要结构，并返回帧值。
                //
                // 如果编码的帧表示无效，则返回错误。这应该终止**当前**连接，但不应影响任何其他连接的客户端。
                let frame = Frame::parse(&mut buf)?;

                // 从读缓冲区中丢弃已解析的数据。
                //
                // 当在读缓冲区上调用 `advance` 时，所有数据都将被丢弃，直到 `len`。关于这是如何工作的详细信息留给了 `BytesMut`。通常，这是通过移动内部光标来完成的，但也可以通过重新分配和复制数据来完成。
                self.buffer.advance(len);

                // 将解析的帧返回给调用方。
                Ok(Some(frame))
            }
            // 在读取缓冲区中没有足够的数据以解析单个帧的情况下，会发生错误。我们必须等待从套接字接收更多数据。从套接字读取将在此 `match` 语句之后执行。
            //
            // 我们不想在这里返回 `Err`，因为这个“错误”是一个预期的运行时条件。
            Err(Incomplete) => Ok(None),
            // 在解析帧时遇到错误。现在连接处于无效状态。在这里返回 `Err` 将导致连接被关闭。
            Err(e) => Err(e.into()),
        }
    }

    /// 将单个 `Frame` 值写入底层流。
    ///
    /// 使用 `AsyncWrite` 提供的各种 `write_*` 函数将 `Frame` 值写入套接字。直接在 `TcpStream` 上调用这些函数**不**是建议的，因为这将导致大量的系统调用。但是，在*缓冲*写流上调用这些函数是可以的。数据将被写入缓冲区。一旦缓冲区满，它就会刷新到底层套接字。
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        // 数组通过编码每个条目来进行编码。所有其他帧类型都被视为字面值。目前，mini-redis 不能编码递归帧结构。有关更多详细信息，请参见下文。
        match frame {
            Frame::Array(val) => {
                // 编码帧类型前缀。对于数组，它是 `*`。
                self.stream.write_u8(b'*').await?;

                // 编码数组的长度。
                self.write_decimal(val.len() as u64).await?;

                // 迭代并编码数组中的每个条目。
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            // 帧类型是字面值。直接编码该值。
            _ => self.write_value(frame).await?,
        }

        // 确保编码的帧被写入套接字。上面的调用是对缓冲流和写入的调用。调用 `flush` 将缓冲区的剩余内容写入套接字。
        self.stream.flush().await
    }

    /// 将帧字面值写入流
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // 不能使用递归策略从值中编码 `Array`。一般来说，异步 fn 不支持递归。mini-redis 尚未需要编码嵌套数组，因此目前跳过。
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    /// 将十进制帧写入流
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // 将值转换为字符串
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
