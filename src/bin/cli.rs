use mini_redis::{clients::Client, DEFAULT_PORT};

use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::convert::Infallible;
use std::num::ParseIntError;
use std::str;
use std::time::Duration;

#[derive(Parser, Debug)]
#[clap(
    name = "mini-redis-cli",
    version,
    author,
    about = "发出 Redis 命令"
)]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[clap(name = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[clap(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        /// 要 ping 的消息
        #[clap(value_parser = bytes_from_str)]
        msg: Option<Bytes>,
    },
    /// 获取键的值。
    Get {
        /// 要获取的键的名称
        key: String,
    },
    /// 将键设置为保存字符串值。
    Set {
        /// 要设置的键的名称
        key: String,

        /// 要设置的值。
        #[clap(value_parser = bytes_from_str)]
        value: Bytes,

        /// 在指定的时间后使值过期
        #[clap(value_parser = duration_from_ms_str)]
        expires: Option<Duration>,
    },
    /// 发布者将消息发送到特定通道。
    Publish {
        /// 通道的名称
        channel: String,

        #[clap(value_parser = bytes_from_str)]
        /// 要发布的消息
        message: Bytes,
    },
    /// 将客户端订阅到特定通道或多个通道。
    Subscribe {
        /// 特定通道或多个通道
        channels: Vec<String>,
    },
}

/// CLI 工具的入口点。
///
/// `[tokio::main]` 注释表示在调用函数时应启动 Tokio 运行时。
/// 函数的主体在新生成的运行时中执行。
///
/// 在这里使用 `flavor = "current_thread"` 是为了避免生成后台线程。
/// CLI 工具的使用情况更适合轻量而不是多线程。
#[tokio::main(flavor = "current_thread")]
async fn main() -> mini_redis::Result<()> {
    // 启用日志记录
    tracing_subscriber::fmt::try_init()?;

    // 解析命令行参数
    let cli = Cli::parse();

    // 获取要连接的远程地址
    let addr = format!("{}:{}", cli.host, cli.port);

    // 建立连接
    let mut client = Client::connect(&addr).await?;

    // 处理请求的命令
    match cli.command {
        Command::Ping { msg } => {
            let value = client.ping(msg).await?;
            if let Ok(string) = str::from_utf8(&value) {
                println!("\"{}\"", string);
            } else {
                println!("{:?}", value);
            }
        }
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }
        Command::Set {
            key,
            value,
            expires: None,
        } => {
            client.set(&key, value).await?;
            println!("OK");
        }
        Command::Set {
            key,
            value,
            expires: Some(expires),
        } => {
            client.set_expires(&key, value, expires).await?;
            println!("OK");
        }
        Command::Publish { channel, message } => {
            client.publish(&channel, message).await?;
            println!("Publish OK");
        }
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                return Err("必须提供通道（们）".into());
            }
            let mut subscriber = client.subscribe(channels).await?;

            // 等待通道上的消息
            while let Some(msg) = subscriber.next_message().await? {
                println!(
                    "从通道收到消息：{}；消息 = {:?}",
                    msg.channel, msg.content
                );
            }
        }
    }

    Ok(())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}

fn bytes_from_str(src: &str) -> Result<Bytes, Infallible> {
    Ok(Bytes::from(src.to_string()))
}
