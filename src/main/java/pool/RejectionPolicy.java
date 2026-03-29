package pool;

/**
 * Политика отказа — определяет, что делать с задачей,
 * когда все очереди переполнены и все потоки заняты.
 */
@FunctionalInterface
public interface RejectionPolicy {
    /**
     * @param task     отклонённая задача
     * @param poolName имя пула (для логирования)
     */
    void reject(Runnable task, String poolName);
}
