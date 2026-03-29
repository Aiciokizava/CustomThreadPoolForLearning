package pool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Кастомный пул потоков с:
 *  - настраиваемым core/max размером
 *  - per-worker очередями
 *  - Round-Robin / Least-Loaded балансировкой
 *  - minSpareThreads
 *  - CallerRuns rejection policy
 *  - подробным логированием
 */
public class CustomThreadPool implements CustomExecutor {

    private static final Logger log = Logger.getLogger(CustomThreadPool.class.getName());

    // ── Конфигурация ──
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;

    // ── Компоненты ──
    private final NamedThreadFactory threadFactory;
    private final TaskDistributor distributor;
    private final RejectionPolicy rejectionPolicy;

    // ── Состояние ──
    private final List<Worker> workers = Collections.synchronizedList(new ArrayList<>());
    private final List<BlockingQueue<Runnable>> queues = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean terminatedNow = new AtomicBoolean(false);
    private final Object lock = new Object();

    // ── Конструктор ──

    public CustomThreadPool(int corePoolSize,
                            int maxPoolSize,
                            long keepAliveTime,
                            TimeUnit timeUnit,
                            int queueSize,
                            int minSpareThreads,
                            TaskDistributor distributor,
                            RejectionPolicy rejectionPolicy,
                            String poolName) {
        if (corePoolSize < 1) throw new IllegalArgumentException("corePoolSize must be >= 1");
        if (maxPoolSize < corePoolSize) throw new IllegalArgumentException("maxPoolSize must be >= corePoolSize");
        if (queueSize < 1) throw new IllegalArgumentException("queueSize must be >= 1");
        if (minSpareThreads < 0) throw new IllegalArgumentException("minSpareThreads must be >= 0");

        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.distributor = distributor;
        this.rejectionPolicy = rejectionPolicy;
        this.threadFactory = new NamedThreadFactory(poolName);

        // Создаём core-потоки
        for (int i = 0; i < corePoolSize; i++) {
            addWorker();
        }
        log.info("[Pool] '" + poolName + "' initialized: core=" + corePoolSize
                + ", max=" + maxPoolSize + ", queueSize=" + queueSize
                + ", minSpare=" + minSpareThreads);
    }

    // ── Создание воркера ──

    private Worker addWorker() {
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(queueSize);
        Worker worker = new Worker(queue, keepAliveTime, timeUnit, this);
        Thread t = threadFactory.newThread(worker);
        worker.setThread(t);
        synchronized (lock) {
            workers.add(worker);
            queues.add(queue);
        }
        t.start();
        return worker;
    }

    // ── execute / submit ──

    @Override
    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException("Task must not be null");
        if (shutdown.get()) {
            log.warning("[Pool] Pool is shut down, rejecting task: " + command);
            rejectionPolicy.reject(command, threadFactory.getPoolName());
            return;
        }

        // Проверяем minSpareThreads — если свободных потоков мало, добавляем
        ensureSpareThreads();

        // Выбираем очередь через distributor
        int idx;
        synchronized (lock) {
            idx = distributor.selectQueue(queues);
        }

        if (idx >= 0) {
            BlockingQueue<Runnable> q;
            synchronized (lock) {
                q = queues.get(idx);
            }
            if (q.offer(command)) {
                log.info("[Pool] Task accepted into queue #" + idx + ": " + command);
                return;
            }
        }

        // Все очереди полны — пробуем расширить пул
        synchronized (lock) {
            if (workers.size() < maxPoolSize) {
                Worker w = addWorker();
                if (w.getQueue().offer(command)) {
                    log.info("[Pool] Scaled up. Task accepted into new queue #"
                            + (workers.size() - 1) + ": " + command);
                    return;
                }
            }
        }

        // Расширяться некуда — rejection
        rejectionPolicy.reject(command, threadFactory.getPoolName());
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) throw new NullPointerException("Callable must not be null");
        FutureTask<T> future = new FutureTask<>(callable);
        execute(future);
        return future;
    }

    // ── Spare threads ──

    private void ensureSpareThreads() {
        synchronized (lock) {
            long idleCount = workers.stream().filter(Worker::isIdle).count();
            while (idleCount < minSpareThreads && workers.size() < maxPoolSize) {
                addWorker();
                idleCount++;
                log.info("[Pool] Added spare thread. Total workers: " + workers.size());
            }
        }
    }

    // ── Shrink logic (вызывается из Worker) ──

    /**
     * Определяет, может ли воркер завершиться по idle-таймауту.
     * Нельзя, если число потоков <= corePoolSize или свободных потоков <= minSpareThreads.
     */
    public boolean canShrink(Worker worker) {
        synchronized (lock) {
            if (workers.size() <= corePoolSize) return false;
            long idleCount = workers.stream().filter(Worker::isIdle).count();
            // Текущий воркер тоже idle, поэтому после его ухода idle станет idleCount-1
            return (idleCount - 1) >= minSpareThreads;
        }
    }

    /** Вызывается воркером при выходе из run(). */
    public void onWorkerExit(Worker worker) {
        synchronized (lock) {
            int idx = workers.indexOf(worker);
            if (idx >= 0) {
                workers.remove(idx);
                queues.remove(idx);
            }
        }
    }

    // ── Shutdown ──

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            log.info("[Pool] Shutdown initiated (graceful). Waiting for queued tasks to complete...");
            synchronized (lock) {
                for (Worker w : workers) {
                    w.stop(); // мягкая остановка
                }
            }
        }
    }

    @Override
    public void shutdownNow() {
        shutdown.set(true);
        terminatedNow.set(true);
        log.info("[Pool] ShutdownNow initiated (forced). Interrupting all workers...");
        synchronized (lock) {
            for (Worker w : workers) {
                w.interrupt();
            }
        }
    }

    // ── Состояние ──

    public boolean isShutdown() {
        return shutdown.get();
    }

    public boolean isTerminatedNow() {
        return terminatedNow.get();
    }

    public boolean isTerminated() {
        return shutdown.get() && workers.isEmpty();
    }

    /**
     * Блокирует вызывающий поток до тех пор, пока все воркеры не завершатся,
     * или пока не истечёт таймаут.
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        // Копируем список потоков, чтобы не держать lock во время join
        List<Thread> threads;
        synchronized (lock) {
            threads = new ArrayList<>();
            for (Worker w : workers) {
                threads.add(w.getThread());
            }
        }
        for (Thread t : threads) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) return isTerminated();
            t.join(TimeUnit.NANOSECONDS.toMillis(remaining));
        }
        return isTerminated();
    }

    // ── Статистика (для отчёта / демо) ──

    public int getWorkerCount() {
        synchronized (lock) {
            return workers.size();
        }
    }

    public int getTotalQueuedTasks() {
        synchronized (lock) {
            return queues.stream().mapToInt(BlockingQueue::size).sum();
        }
    }

    public String getPoolName() {
        return threadFactory.getPoolName();
    }
}
