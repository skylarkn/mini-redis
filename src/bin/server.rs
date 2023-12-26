//! mini-redis 服务器。
//!
//! 该文件是库中实现的服务器的入口点。它执行命令行解析并将参数传递给 `mini_redis::server`。
//!
//! 使用 `clap` crate 进行参数解析。

use mini_redis::{server, DEFAULT_PORT};

use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

#[cfg(feature = "otel")]
// 用于设置 XrayPropagator
use opentelemetry::global;
#[cfg(feature = "otel")]
// 用于配置某些选项，如采样率
use opentelemetry::sdk::trace as sdktrace;
#[cfg(feature = "otel")]
// 用于在服务之间传递相同的 XrayId
use opentelemetry_aws::trace::XrayPropagator;
#[cfg(feature = "otel")]
// `Ext` traits 用于允许 Registry 接受 OpenTelemetry 特定类型（如 `OpenTelemetryLayer`）
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, util::TryInitError, EnvFilter,
};

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // 绑定 TCP 监听器
    // 使用Tokio的TcpListener绑定到指定IP地址和端口上。这是一个异步操作，所以使用await关键字。
    // ?是一个用于传播错误的快捷方式，如果发生错误，将立即返回Err。
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

/// 这里使用了derive宏，它会自动为结构体实现一些 trait，其中包括Parser和Debug。Parser trait 是由 clap 提供的，用于生成命令行解析器。
#[derive(Parser, Debug)]
/// 这是clap宏的配置部分，用于配置命令行解析器的元数据。具体来说，设置了应用程序的名称（name）、版本号（version）、作者（author）和简介（about）。
#[clap(name = "mini-redis-server", version, author, about = "A Redis server")]
struct Cli {
    // 定义了一个结构体Cli，它包含一个字段port，类型是Option<u16>。
    // 这里使用了#[clap(long)]属性，它告诉clap库在解析命令行参数时要考虑port字段，并且使用--port这样的长格式命令行参数。
    #[clap(long)]
    port: Option<u16>,
}

#[cfg(not(feature = "otel"))]
fn set_up_logging() -> mini_redis::Result<()> {
    // 有关更多信息，请参阅 https://docs.rs/tracing
    tracing_subscriber::fmt::try_init()
}

#[cfg(feature = "otel")]
fn set_up_logging() -> Result<(), TryInitError> {
    // 将全局传播器设置为 X-Ray 传播器
    // 注意：如果需要在同一跟踪中在服务之间传递 x-amzn-trace-id，您将需要此行。
    // 但是，这需要未在此处显示的额外代码。有关使用 hyper 的完整示例，请参见：
    // https://github.com/open-telemetry/opentelemetry-rust/blob/main/examples/aws-xray/src/server.rs#L14-L26
    global::set_text_map_propagator(XrayPropagator::default());

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(
            sdktrace::config()
                .with_sampler(sdktrace::Sampler::AlwaysOn)
                // 为了将跟踪 ID 转换为与 Xray 兼容的格式，需要此项
                .with_id_generator(sdktrace::XrayIdGenerator::default()),
        )
        .install_simple()
        .expect("Unable to initialize OtlpPipeline");

    // 使用配置的追踪器创建一个追踪层
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // 从 `RUST_LOG` 环境变量中解析 `EnvFilter` 配置
    let filter = EnvFilter::from_default_env();

    // 使用追踪订阅者 `Registry` 或其他实现了 `LookupSpan` 的订阅者
    tracing_subscriber::registry()
        .with(opentelemetry)
        .with(filter)
        .with(fmt::Layer::default())
        .try_init()
}
