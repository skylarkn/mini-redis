use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// 对 `Db` 实例的包装。此结构存在的目的是在此结构被丢弃时向后台清理任务发出关闭信号，以便有序地清理 `Db`。
#[derive(Debug)]
/// `DbDropGuard` 的结构体，用于在此 `DbDropGuard` 结构体被丢弃时关闭的 `Db` 实例。
pub(crate) struct DbDropGuard {
    /// 将在此 `DbDropGuard` 结构体被丢弃时关闭的 `Db` 实例。
    db: Db,
}

/// 共享所有连接的服务器状态。
///
/// `Db` 包含一个存储键/值数据以及所有活动发布/订阅通道的 `broadcast::Sender` 值的 `HashMap`。
///
/// `Db` 实例是共享状态的句柄。克隆 `Db` 是浅层的，只会发生原子引用计数的增加。
///
/// 当创建一个 `Db` 值时，会生成一个后台任务。该任务用于在请求的持续时间过去后使值过期。任务运行直到所有 `Db` 实例被丢弃，此时任务终止。
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// 共享状态的句柄。后台任务也将具有一个 `Arc<Shared>`。
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// 共享状态由互斥锁保护。这是一个 `std::sync::Mutex` 而不是 Tokio 互斥锁。这是因为在持有互斥锁时没有执行异步操作。此外，关键部分非常小。
    ///
    /// Tokio 互斥锁主要用于在 `.await` yield 点之间需要持有锁的情况。所有其他情况 **通常** 最好使用 std 互斥锁。如果关键部分不包含任何异步操作但很长（CPU 密集型或执行阻塞操作），则整个操作，包括等待互斥锁，被视为“阻塞”操作，并应使用 `tokio::task::spawn_blocking`。
    state: Mutex<State>,

    /// 通知处理条目过期的后台任务。后台任务等待此通知，然后检查过期值或关闭信号。
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// 键值数据。我们不打算做任何花哨的事情，所以 `std::collections::HashMap` 完全可以工作。
    entries: HashMap<String, Entry>,

    /// 发布/订阅键空间。Redis 使用一个 **单独的** 键空间用于键值和发布/订阅。`mini-redis` 通过使用一个单独的 `HashMap` 来处理这一点。
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// 跟踪键的 TTL。
    ///
    /// 使用 `BTreeSet` 来维护按过期时间排序的到期项。这允许后台任务迭代此映射以查找下一个到期的值。
    ///
    /// 虽然极不可能，但有可能为同一瞬间创建多个到期。因此，`Instant` 不足以表示键。使用唯一键（`String`）来打破这些联系。
    expirations: BTreeSet<(Instant, String)>,

    /// 当 Db 实例正在关闭时为 true。当所有 `Db` 值都被丢弃时会发生这种情况。将其设置为 `true` 会向后台任务发出退出信号。
    shutdown: bool,
}

/// 键值存储中的条目
#[derive(Debug)]
struct Entry {
    /// 存储的数据
    data: Bytes,

    /// 条目过期并应从数据库中删除的时刻。
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    /// 创建一个新的 `DbHolder`，包装一个 `Db` 实例。当此结构被丢弃时，`Db` 的清理任务将被关闭。
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// 获取共享数据库。内部是一个 `Arc`，因此克隆只会增加引用计数。
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // 向 'Db' 实例发出信号，关闭清理过期键的任务
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// 创建一个新的、空的 `Db` 实例。分配共享状态并启动一个后台任务来管理键的过期。
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // 启动后台任务。
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// 获取与键关联的值。
    ///
    /// 如果键没有关联的值，则返回 `None`。这可能是由于从未为键分配值，或者先前分配的值已过期。
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 获取锁，获取条目并克隆值。
        //
        // 由于使用 `Bytes` 存储数据，在这里的克隆是浅层克隆。数据不会被复制。
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// 设置与键关联的值以及可选的过期持续时间。
    ///
    /// 如果键已经关联了一个值，它将被删除。
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // 如果此 `set` 成为**下一个**到期的键，则需要通知后台任务，以便它可以更新其状态。
        //
        // 是否需要通知任务是在 `set` 过程中计算的。
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // 键过期的 `Instant`。
            let when = Instant::now() + duration;

            // 仅当新插入的到期时间是**下一个**要驱逐的键时，才通知工作任务。在这种情况下，需要唤醒工作任务以更新其状态。
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            when
        });

        // 将条目插入 `HashMap`。
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        // 如果先前关联了键的值**并且**它有一个到期时间。必须还删除 `expirations` 映射中的关联条目。这样可以避免数据泄漏。
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // 清除到期时间
                state.expirations.remove(&(when, key.clone()));
            }
        }

        // 跟踪到期时间。如果在移除之前插入，则当当前 `(when, key)` 等于先前的 `(when, key)` 时将导致错误。移除然后插入可以避免这种情况。
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }

        // 通知后台任务之前释放互斥锁。这有助于减少争用，避免后台任务醒来只是因为此函数仍然保持着互斥锁而无法获取它。
        drop(state);

        if notify {
            // 最后，只有在需要更新其状态以反映新的到期时间时才通知后台任务。
            self.shared.background_task.notify_one();
        }
    }

    /// 返回所请求通道的 `Receiver`。
    ///
    /// 返回的 `Receiver` 用于接收由 `PUBLISH` 命令广播的值。
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // 获取互斥锁
        let mut state = self.shared.state.lock().unwrap();

        // 如果请求通道的条目不存在，则创建一个新的广播通道并将其与键关联。如果已经存在，则返回关联的接收器。
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // 尚不存在广播通道，因此创建一个。
                //
                // 该通道的容量为 `1024` 条消息。消息存储在通道中，直到**所有**订阅者都看到它。这意味着慢的订阅者可能导致消息无限期地保持。
                //
                // 当通道的容量填满时，发布将导致旧消息被丢弃。这可防止慢速消费者阻塞整个系统。
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// 向通道发布消息。返回正在侦听通道的订阅者数量。
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // 在成功发送广播通道上的消息时，返回订阅者的数量。错误表示没有接收器，在这种情况下应返回 `0`。
            .map(|tx| tx.send(value).unwrap_or(0))
            // 如果通道键没有条目，则没有订阅者。在这种情况下，返回 `0`。
            .unwrap_or(0)
    }

    /// 通知清理后台任务关闭。由 `DbShutdown` 的 `Drop` 实现调用。
    fn shutdown_purge_task(&self) {
        // 必须通知后台任务关闭。这是通过将 `State::shutdown` 设置为 `true` 并发出信号来完成的。
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // 在通知后台任务之前释放锁。这有助于减少锁争用，确保后台任务不会只是醒来而无法获取互斥锁。
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// 清除所有过期的键并返回**下一个**键将到期的 `Instant`。后台任务将休眠直到此时刻。
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // 数据库正在关闭。所有对共享状态的句柄都已经丢失。后台任务应该退出。
            return None;
        }

        // 为了使借用检查器满意，这是必需的。简而言之，`lock()` 返回一个 `MutexGuard` 而不是 `&mut State`。借用检查器无法“看透”互斥锁保护并确定可以安全地可变地访问 `state.expirations` 和 `state.entries`，因此我们在循环外部得到了“真正的”可变引用到 `State`。
        let state = &mut *state;

        // 查找所有在现在之前计划过期的键。
        let now = Instant::now();

        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                // 清理完成，`when` 是**下一个**键到期的瞬间。工作任务将等到此瞬间。
                return Some(when);
            }

            // 键过期，删除它
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }

    /// 如果数据库正在关闭，则返回 `true`
    ///
    /// 当所有 `Db` 值都被丢弃时，设置 `shutdown` 标志，表示无法再访问共享状态。
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// 后台任务执行的例程。
///
/// 等待通知。在通知时，从共享状态句柄中清理任何过期的键。如果设置了 `shutdown`，则终止任务。
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // 如果设置了关闭标志，则任务应退出。
    while !shared.is_shutdown() {
        // 清理所有过期的键。该函数返回**下一个**键将到期的瞬间。工作者应该等到这个瞬间过去然后再次清理。
        if let Some(when) = shared.purge_expired_keys() {
            // 等到下一个键过期**或**后台任务被通知。如果任务被通知，则必须重新加载其状态，因为新的键被设置为提前到期。这是通过循环完成的。
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // 没有在未来到期的键。等到任务被通知。
            shared.background_task.notified().await;
        }
    }

    debug!("清理后台任务已关闭")
}
