use crate::Frame;

use bytes::Bytes;
use std::{fmt, str, vec};

/// 用于解析命令的实用工具
///
/// 命令被表示为数组帧。帧中的每个条目都是一个“标记”（token）。`Parse` 被初始化为数组帧，
/// 并提供类似游标的 API。每个命令结构都包含一个 `parse_frame` 方法，该方法使用 `Parse` 提取其字段。
#[derive(Debug)]
pub(crate) struct Parse {
    /// 数组帧迭代器。
    parts: vec::IntoIter<Frame>,
}

/// 解析帧时遇到的错误。
///
/// 只有 `EndOfStream` 错误在运行时被处理。所有其他错误导致连接被终止。
#[derive(Debug)]
pub(crate) enum ParseError {
    /// 由于帧已完全消耗而尝试提取值失败。
    EndOfStream,

    /// 所有其他错误
    Other(crate::Error),
}

impl Parse {
    /// 创建一个新的 `Parse` 以解析 `frame` 的内容。
    ///
    /// 如果 `frame` 不是数组帧，则返回 `Err`。
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("协议错误；期望数组，得到 {:?}", frame).into()),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    /// 返回下一个条目。数组帧是帧的数组，因此下一个条目是帧。
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    /// 将下一个条目作为字符串返回。
    ///
    /// 如果下一个条目不能表示为字符串，则返回错误。
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            // `Simple` 和 `Bulk` 表示都可以是字符串。字符串被解析为 UTF-8。
            //
            // 虽然错误被存储为字符串，但它们被视为不同的类型。
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "协议错误；无效的字符串".into()),
            frame => Err(format!(
                "协议错误；期望简单帧或块帧，得到 {:?}",
                frame
            )
            .into()),
        }
    }

    /// 将下一个条目作为原始字节返回。
    ///
    /// 如果下一个条目不能表示为原始字节，则返回错误。
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            // `Simple` 和 `Bulk` 表示都可以是原始字节。
            //
            // 虽然错误被存储为字符串并且可以表示为原始字节，但它们被视为不同的类型。
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!(
                "协议错误；期望简单帧或块帧，得到 {:?}",
                frame
            )
            .into()),
        }
    }

    /// 将下一个条目作为整数返回。
    ///
    /// 这包括 `Simple`、`Bulk` 和 `Integer` 帧类型。将解析 `Simple` 和
    /// `Bulk` 帧类型。
    ///
    /// 如果下一个条目不能表示为整数，则返回错误。
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;

        const MSG: &str = "协议错误；无效的数字";

        match self.next()? {
            // 整数帧类型已经存储为整数。
            Frame::Integer(v) => Ok(v),
            // 必须将 `Simple` 和 `Bulk` 帧类型解析为整数。如果解析失败，则返回错误。
            Frame::Simple(data) => atoi::<u64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!("协议错误；期望整数帧但得到 {:?}", frame).into()),
        }
    }

    /// 确保数组中没有更多条目
    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("协议错误；期望帧结束，但有更多内容".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "协议错误；意外的流结束".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}
