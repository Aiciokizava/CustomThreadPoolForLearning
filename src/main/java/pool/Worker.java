package pool;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Рабочий поток. Берёт задачи из закреплённой очереди.
 * Если задач нет дольше keepAliveTime и число потоков > corePoolSize — завершается.
 */
public class Worker implements Runnable {

    private static final Logger log = Logger.getLogger(Worker.class.getName());

    private final BlockingQueue<Runnable> queue;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final CustomThreadPool pool;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private Thread thread; // ссылка на поток, в котором запущен данный Worker

    public Worker(BlockingQueue<Runnable> queue,
                  long keepAliveTime,
                  TimeUnit timeUnit,
                  CustomThreadPool pool) {
        this.queue = queue;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.pool = pool;
    }

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    public Thread getThread() {
        return thread;
    }

    public BlockingQueue<Runnable> getQueue() {
        return queue;
    }

    /** Мягкая остановка: воркер завершится после текущей задачи. */
    public void stop() {
        running.set(false);
    }

    /** Жёсткая остановка: interrupt потока. */
    public void interrupt() {
        running.set(false);
        if (thread != null) {
            thread.interrupt();
        }
    }

    /** Проверяет, свободен ли воркер (очередь пуста). */
    public boolean isIdle() {
        return queue.isEmpty();
    }

    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        log.info("[Worker] " + name + " started.");

        try {
            while (running.get() && !pool.isTerminated()) {
                Runnable task;
                try {
                    task = queue.poll(keepAliveTime, timeUnit);
                } catch (InterruptedException e) {
                    // Если прервали — проверяем, нужно ли завершаться
                    if (!running.get() || pool.isShutdown()) {
                        break;
                    }
                    continue;
                }

                if (task != null) {
                    try {
                        log.info("[Worker] " + name + " executes " + task);
                        task.run();
                    } catch (Exception e) {
                        log.severe("[Worker] " + name + " task threw exception: " + e.getMessage());
                    }
                } else {
                    // Таймаут — задач не было keepAliveTime
                    if (pool.canShrink(this)) {
                        log.info("[Worker] " + name + " idle timeout, stopping.");
                        break;
                    }
                    // Иначе — это core-поток или нужен как spare, продолжаем ждать
                }
            }

            // Перед выходом дорабатываем оставшиеся задачи в очереди (graceful)
            if (pool.isShutdown() && !pool.isTerminatedNow()) {
                Runnable remaining;
                while ((remaining = queue.poll()) != null) {
                    try {
                        log.info("[Worker] " + name + " (draining) executes " + remaining);
                        remaining.run();
                    } catch (Exception e) {
                        log.severe("[Worker] " + name + " drain task threw exception: " + e.getMessage());
                    }
                }
            }
        } finally {
            pool.onWorkerExit(this);
            log.info("[Worker] " + name + " terminated.");
        }
    }
}
