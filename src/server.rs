//! 最小化Redis服务器实现
//!
//! 提供一个异步的`run`函数，用于监听入站连接，为每个连接生成一个任务。

use crate::{Command, Connection, Db, DbDropGuard, Shutdown};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

/// 服务器监听状态。在`run`调用中创建。它包括一个`run`方法，执行TCP监听和每个连接的初始化。
#[derive(Debug)]
struct Listener {
    /// 共享数据库句柄。
    ///
    /// 包含键/值存储以及用于发布/订阅的广播通道。
    ///
    /// 这个字段包装了一个`Arc`的内部`Db`可以被检索并传递到每个连接的状态(`Handler`)中。
    db_holder: DbDropGuard,

    /// 由`run`调用者提供的TCP监听器。
    listener: TcpListener,

    /// 限制最大连接数。
    ///
    /// 使用`Semaphore`来限制最大连接数。在尝试接受新连接之前，需要从信号量获取一个许可证。如果没有可用的许可证，监听器将等待一个。
    ///
    /// 当处理完连接的处理程序时，将许可证返回到信号量。
    limit_connections: Arc<Semaphore>,

    /// 向所有活动连接广播关闭信号。
    ///
    /// 初始的`shutdown`触发器由`run`调用者提供。服务器负责优雅地关闭活动连接。
    /// 当生成连接任务时，它会传递一个广播接收器句柄。当启动优雅关闭时，通过广播发送一个`()`值。
    /// 每个活动连接都会收到它，达到安全的终端状态后完成任务。
    notify_shutdown: broadcast::Sender<()>,

    /// 用于优雅关闭过程的等待客户端连接完成处理。
    ///
    /// 当所有`Sender`句柄超出范围时，Tokio通道将关闭。
    /// 当通道关闭时，接收器将接收`None`。这被用于检测所有连接处理程序的完成。当连接处理程序初始化时，它会被分配一个`shutdown_complete_tx`字段的克隆。
    /// 当监听器关闭时，它放弃了`shutdown_complete_tx`字段持有的发送方。一旦所有处理程序任务完成，`Sender`的所有克隆也将被丢弃。这导致`shutdown_complete_rx.recv()`完成为`None`。此时，安全地退出服务器进程。
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// 每个连接处理程序。从`connection`读取请求并将命令应用于`db`。
#[derive(Debug)]
struct Handler {
    /// 共享数据库句柄。
    ///
    /// 当从`connection`接收到命令时，将使用`db`应用它。命令的实现在`cmd`模块中。每个命令都需要与`db`交互以完成工作。
    db: Db,

    /// 使用实现了带有缓冲的`TcpStream`的Redis协议编码器/解码器的TCP连接。
    ///
    /// 当`Listener`接收到入站连接时，将`TcpStream`传递给`Connection::new`，该函数初始化相关的缓冲区。
    /// `Connection`允许处理程序在“帧”级别操作，并将字节级协议解析详细信息封装在`Connection`中。
    connection: Connection,

    /// 监听关闭通知。
    ///
    /// 封装在`Listener`中的`broadcast::Receiver`与`Listener`中的发送方配对。连接处理程序处理来自连接的请求，直到对`shutdown`的关闭通知**或**从`shutdown`接收到关闭通知。在后一种情况下，正在处理的对等方的任何正在进行中的工作都会继续进行，直到达到安全状态，此时连接终止。
    shutdown: Shutdown,

    /// 不直接使用。相反，当`Handler`被释放时……？
    _shutdown_complete: mpsc::Sender<()>,
}

/// Redis服务器将接受的最大并发连接数。
///
/// 当达到此限制时，服务器将停止接受连接，直到活动连接终止。
///
/// 实际应用程序将希望使此值可配置，但对于此示例，它是硬编码的。
///
/// 这也设置为一个相当低的值，以阻止在生产中使用（你可能认为所有的免责声明都会使人们明白这不是一个严肃的项目……但我对mini-http也是这么想的）。
const MAX_CONNECTIONS: usize = 250;

/// 运行mini-redis服务器。
///
/// 从提供的监听器接受连接。对于每个入站连接，将生成一个任务来处理该连接。服务器运行直到`shutdown`未完成，此时服务器将优雅地关闭。
///
/// `tokio::signal::ctrl_c()`可以用作`shutdown`参数。这将监听SIGINT信号。
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // 当提供的`shutdown`未完成时，我们必须向所有活动连接发送关闭消息。我们使用广播通道来实现这一目的。
    // 以下调用忽略了广播对的接收器，当需要接收器时，可以使用发送方上的subscribe()方法创建一个。
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    // 初始化监听器状态
    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    // 并发运行服务器并监听`shutdown`信号。任务在遇到错误时运行，因此在正常情况下，此`select!`语句运行直到接收到`shutdown`信号。
    //
    // `select!`语句的形式如下：
    //
    // ```
    // <async op> = <async op> => <step to perform with result>
    // ```
    //
    // 所有`<async op>`语句都是并行执行的。一旦第一个操作完成，将执行其关联的`<step to perform with result>`。
    //
    // `select!`宏是编写异步Rust的基础构建块之一。有关更多详细信息，请参见API文档：
    //
    // https://docs.rs/tokio/*/tokio/macro.select.html
    tokio::select! {
        res = server.run() => {
            // 如果在这里收到错误，表示从TCP监听器接受连接多次失败，服务器正在放弃并关闭。
            //
            // 处理单个连接时遇到的错误不会冒泡到此处。
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // 已收到关闭信号。
            info!("shutting down");
        }
    }

    // 显式提取`shutdown_complete`接收器和发送器
    // 显式删除`shutdown_transmitter`。这很重要，因为下面的`.await`否则永远不会完成。
    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // 当`notify_shutdown`被丢弃时，所有已订阅的任务将接收到关闭信号，并可以退出
    drop(notify_shutdown);
    // 删除最后的`Sender`以便下面的`Receiver`可以完成
    drop(shutdown_complete_tx);

    // 等待所有活动连接完成处理。由于监听器上面持有的`Sender`句柄已经被丢弃，唯一剩下的`Sender`实例是连接处理程序任务持有的。
    // 当它们被丢弃时，`mpsc`通道将关闭，并且`recv()`将返回`None`。
    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    /// 运行服务器
    ///
    /// 监听入站连接。对于每个入站连接，生成一个处理该连接的任务。
    ///
    /// # 错误
    ///
    /// 如果接受返回错误，则返回`Err`。这可能由于多种原因而随着时间的推移解决。例如，如果底层操作系统已达到最大套接字数的内部限制，则接受将失败。
    ///
    /// 进程无法检测到瞬态错误何时解决自身。处理这种情况的一种策略是实现退避策略，这正是我们在这里做的。
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            // 等待许可证可用
            //
            // `acquire_owned`返回一个许可证，该许可证绑定到信号量。当许可证值被丢弃时，它将自动返回到信号量。
            //
            // `acquire_owned()`在信号量被关闭时返回`Err`。我们永远不会关闭信号量，因此`unwrap()`是安全的。
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            // 接受新套接字。这将尝试执行错误处理。
            // `accept`方法内部尝试恢复错误，因此此处的错误是不可恢复的。
            let socket = self.accept().await?;

            // 创建所需的每个连接处理程序状态。
            let mut handler = Handler {
                // 获取共享数据库的句柄。
                db: self.db_holder.db(),

                // 初始化连接状态。这会为执行redis协议帧解析分配读/写缓冲区。
                connection: Connection::new(socket),

                // 接收关闭通知。
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),

                // 一旦克隆全部被删除，通知接收器的不使用。
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // 生成一个新任务以处理连接。Tokio任务类似于异步的绿色线程，并且是并发执行的。
            tokio::spawn(async move {
                // 处理连接。如果遇到错误，则记录它。
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
                // 将许可证移到任务中并在完成后将其丢弃。这将许可证返回到信号量。
                drop(permit);
            });
        }
    }

    /// 接受入站连接。
    ///
    /// 错误通过后退和重试来处理。使用指数后退策略。第一次失败后，任务将等待1秒。第二次失败后，任务将等待2秒。每次后续失败都会使等待时间加倍。如果在等待64秒后第6次尝试接受失败，那么此函数将带有错误返回。
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // 尝试接受几次
        loop {
            // 执行接受操作。如果成功接受套接字，则返回它。否则，保存错误。
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // 接受失败太多次。返回错误。
                        return Err(err.into());
                    }
                }
            }

            // 暂停执行直到后退期间过去。
            time::sleep(Duration::from_secs(backoff)).await;

            // 将后退加倍
            backoff *= 2;
        }
    }
}

impl Handler {
    /// 处理单个连接。
    ///
    /// 从套接字读取请求帧并进行处理。响应被写回套接字。
    ///
    /// 目前，未实现流水线处理。流水线处理是指能够在每个连接上并发处理多个请求，而无需交错帧的能力。更多细节请参见：
    /// https://redis.io/topics/pipelining
    ///
    /// 当接收到关闭信号时，连接会被处理直到达到安全状态，然后终止。
    
    /// crate 提供的属性宏，用于标记一个异步函数或方法，并自动生成日志记录（logging）代码以记录函数的执行。这个宏的目的是简化日志记录的添加，使其与异步 Rust 代码更加兼容。
    /// 具体来说，#[instrument(skip(self))] 在生成的日志记录中表明，不需要记录这个函数中的 self 参数的详细信息。
    /// 这可以减少日志输出的冗余，尤其是当 self 是一个包含大量信息的结构体时。
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        // 只要未收到关闭信号，就尝试读取新的请求帧。
        while !self.shutdown.is_shutdown() {
            // 在读取请求帧的同时，也监听关闭信号。
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // 如果收到关闭信号，就从 `run` 中返回。
                    // 这将导致任务终止。
                    return Ok(());
                }
            };

            // 如果从 `read_frame()` 返回 `None`，则对等方关闭了套接字。
            // 没有进一步的工作要做，任务可以终止。
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // 将 Redis 帧转换为命令结构。如果帧不是有效的 Redis 命令或是不支持的命令，则返回错误。
            let cmd = Command::from_frame(frame)?;

            // 记录 `cmd` 对象。此处的语法是由 `tracing` crate 提供的简写。可以视为类似于：
            //
            // ```
            // debug!(cmd = format!("{:?}", cmd));
            // ```
            //
            // `tracing` 提供了结构化日志记录，因此信息被记录为键值对。
            debug!(?cmd);

            // 执行应用命令所需的工作。这可能会由于此操作导致数据库状态发生变化。
            //
            // 连接被传递到应用函数，允许命令直接向连接写入响应帧。在 pub/sub 的情况下，可能会向对等方发送多个帧。
            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                .await?;
        }

        Ok(())
    }
}
