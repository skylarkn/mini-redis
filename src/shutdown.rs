use tokio::sync::broadcast;

/// 监听服务器关闭信号。
///
/// 使用 `broadcast::Receiver` 发送关闭信号。仅发送一个值。一旦通过广播信道发送了一个值，服务器就应该关闭。
///
/// `Shutdown` 结构监听信号并跟踪信号是否已接收。调用者可以查询是否已接收关闭信号。
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// 如果关闭信号已接收，则为 `true`
    is_shutdown: bool,

    /// 用于监听关闭的通道的接收端。
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// 创建一个由给定的 `broadcast::Receiver` 支持的新的 `Shutdown` 实例。
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    /// 如果已接收关闭信号，则返回 `true`。
    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// 接收关闭通知，必要时等待。
    pub(crate) async fn recv(&mut self) {
        // 如果已经接收到关闭信号，则立即返回。
        if self.is_shutdown {
            return;
        }

        // 无法接收“滞后错误”，因为只发送一个值。
        let _ = self.notify.recv().await;

        // 记住已接收到信号。
        self.is_shutdown = true;
    }
}
