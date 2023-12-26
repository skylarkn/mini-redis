//! 一个极简（即非常不完整）的 Redis 服务器和客户端实现。
//!
//! 该项目的目的是提供一个使用 Tokio 构建的异步 Rust 项目的较大示例。不要尝试在生产环境中运行这个……真的。
//!
//! # 布局
//!
//! 该库的结构使其可以与指南一起使用。有一些模块是公共的，可能在一个“真实”的 Redis 客户端库中不是公共的。
//!
//! 主要组件有：
//!
//! * `server`：Redis 服务器实现。包括一个 `run` 函数，接受一个 `TcpListener` 并开始接受 Redis 客户端连接。
//!
//! * `clients/client`：一个异步 Redis 客户端实现。演示了如何使用 Tokio 构建客户端。
//!
//! * `cmd`：支持的 Redis 命令的实现。
//!
//! * `frame`：表示单个 Redis 协议帧。帧被用作在“命令”和字节表示之间的中间表示。
//!
//! ```rust
//! pub mod clients;
//! pub use clients::{BlockingClient, BufferedClient, Client};
//!
//! pub mod cmd;
//! pub use cmd::Command;
//!
//! mod connection;
//! pub use connection::Connection;
//!
//! pub mod frame;
//! pub use frame::Frame;
//!
//! mod db;
//! use db::Db;
//! use db::DbDropGuard;
//!
//! mod parse;
//! use parse::{Parse, ParseError};
//!
//! pub mod server;
//!
//! mod shutdown;
//! use shutdown::Shutdown;
//! ```

pub mod clients;
pub use clients::{BlockingClient, BufferedClient, Client};

pub mod cmd;
pub use cmd::Command;

mod connection;
pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod db;
use db::Db;
use db::DbDropGuard;

mod parse;
use parse::{Parse, ParseError};

pub mod server;

mod shutdown;
use shutdown::Shutdown;

/// Redis 服务器监听的默认端口。
///
/// 如果没有指定端口，将使用此端口。
pub const DEFAULT_PORT: u16 = 6379;

/// 大多数函数返回的错误类型。
///
/// 在编写实际应用程序时，可能希望考虑使用专门的错误处理库或定义一个错误类型作为原因的 `enum`。但是，对于我们的示例来说，使用包装的 `std::error::Error` 已经足够了。
///
/// 出于性能原因，在任何热点路径中都会避免使用装箱。例如，在 `parse` 中，定义了一个自定义的错误 `enum`。这是因为在正常执行期间，当在套接字上接收到部分帧时，会遇到并处理错误。为 `parse::Error` 实现了 `std::error::Error`，这使得它可以转换为 `Box<dyn std::error::Error>`。
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// 用于 mini-redis 操作的专用 `Result` 类型。
///
/// 这被定义为一种方便。
pub type Result<T> = std::result::Result<T, Error>;
