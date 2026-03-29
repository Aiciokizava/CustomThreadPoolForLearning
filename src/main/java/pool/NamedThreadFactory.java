package pool;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 *  Присваивает уникальные имена, логирует создание и завершение.
 */
public class NamedThreadFactory {

    private static final Logger log = Logger.getLogger(NamedThreadFactory.class.getName());

    private final String poolName;
    private final AtomicInteger counter = new AtomicInteger(0);

    public NamedThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    /**
     * Создаёт новый поток-демон с уникальным именем.
     * При завершении потока (uncaughtException или нормальный выход)
     * событие логируется.
     */
    public Thread newThread(Runnable target) {
        int id = counter.incrementAndGet();
        String name = poolName + "-worker-" + id;
        log.info("[ThreadFactory] Creating new thread: " + name);

        Thread t = new Thread(target, name);
        t.setDaemon(false);
        t.setUncaughtExceptionHandler((thread, ex) ->
                log.severe("[ThreadFactory] Uncaught exception in " + thread.getName() + ": " + ex.getMessage()));
        return t;
    }

    public String getPoolName() {
        return poolName;
    }
}
