package pool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;

public class Main {

    // Счётчик для красивых имён задач
    private static final AtomicInteger taskCounter = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {

        // ── Настройка логирования ──
        setupLogging();

        Logger log = Logger.getLogger(Main.class.getName());

        // ═══════════════════════════════════════════════════════
        // 1. Демонстрация базовой работы пула
        // ═══════════════════════════════════════════════════════
        log.info("=== DEMO 1: Basic pool operation (RoundRobin) ===");

        CustomThreadPool pool = new CustomThreadPool(
                /* corePoolSize   */ 2,
                /* maxPoolSize    */ 4,
                /* keepAliveTime  */ 5,
                /* timeUnit       */ TimeUnit.SECONDS,
                /* queueSize      */ 5,
                /* minSpareThreads*/ 1,
                new RoundRobinDistributor(),
                new CallerRunsOnRejectPolicy(),
                "MyPool"
        );

        // Отправляем 8 задач
        for (int i = 0; i < 8; i++) {
            final int taskId = taskCounter.incrementAndGet();
            pool.execute(new NamedTask("Task-" + taskId, 1000 + (int)(Math.random() * 2000)));
        }

        // Подождём немного и посмотрим статистику
        Thread.sleep(2000);
        log.info("[Main] Workers alive: " + pool.getWorkerCount()
                + ", queued tasks: " + pool.getTotalQueuedTasks());

        // ═══════════════════════════════════════════════════════
        // 2. Демонстрация submit + Future
        // ═══════════════════════════════════════════════════════
        log.info("=== DEMO 2: submit(Callable) with Future ===");

        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            final int taskId = taskCounter.incrementAndGet();
            futures.add(pool.submit(() -> {
                String name = "Callable-" + taskId;
                log.info("[Callable] " + name + " running in " + Thread.currentThread().getName());
                Thread.sleep(500);
                return name + " result OK";
            }));
        }

        for (Future<String> f : futures) {
            log.info("[Main] Future result: " + f.get());
        }

        // ═══════════════════════════════════════════════════════
        // 3. Демонстрация перегрузки (rejection)
        // ═══════════════════════════════════════════════════════
        log.info("=== DEMO 3: Overload / Rejection ===");

        // Создаём маленький пул, чтобы быстро переполнить
        CustomThreadPool tinyPool = new CustomThreadPool(
                1, 2, 3, TimeUnit.SECONDS,
                2, 0,
                new LeastLoadedDistributor(),
                new CallerRunsOnRejectPolicy(),
                "TinyPool"
        );

        // Отправляем 15 задач — часть будет отклонена (CallerRuns)
        for (int i = 0; i < 15; i++) {
            final int taskId = taskCounter.incrementAndGet();
            tinyPool.execute(new NamedTask("Tiny-" + taskId, 800));
        }

        // ═══════════════════════════════════════════════════════
        // 4. Graceful shutdown
        // ═══════════════════════════════════════════════════════
        log.info("=== DEMO 4: Graceful shutdown ===");

        pool.shutdown();
        boolean terminated = pool.awaitTermination(30, TimeUnit.SECONDS);
        log.info("[Main] MyPool terminated: " + terminated);

        tinyPool.shutdown();
        terminated = tinyPool.awaitTermination(30, TimeUnit.SECONDS);
        log.info("[Main] TinyPool terminated: " + terminated);

        // ═══════════════════════════════════════════════════════
        // 5. Демонстрация idle-timeout (shrink)
        // ═══════════════════════════════════════════════════════
        log.info("=== DEMO 5: Idle timeout / shrink ===");

        CustomThreadPool shrinkPool = new CustomThreadPool(
                1, 4, 3, TimeUnit.SECONDS,
                3, 0,
                new RoundRobinDistributor(),
                new CallerRunsOnRejectPolicy(),
                "ShrinkPool"
        );

        // Создаём нагрузку, чтобы пул расширился
        for (int i = 0; i < 12; i++) {
            final int taskId = taskCounter.incrementAndGet();
            shrinkPool.execute(new NamedTask("Shrink-" + taskId, 500));
        }

        log.info("[Main] ShrinkPool workers after burst: " + shrinkPool.getWorkerCount());

        // Ждём, пока idle-потоки завершатся
        Thread.sleep(10_000);
        log.info("[Main] ShrinkPool workers after idle: " + shrinkPool.getWorkerCount());

        shrinkPool.shutdown();
        shrinkPool.awaitTermination(10, TimeUnit.SECONDS);
        log.info("[Main] ShrinkPool terminated.");

        // ═══════════════════════════════════════════════════════
        // 6. Сравнение с ThreadPoolExecutor (бенчмарк)
        // ═══════════════════════════════════════════════════════
        log.info("=== DEMO 6: Benchmark vs ThreadPoolExecutor ===");
        benchmark();

        log.info("=== ALL DEMOS COMPLETE ===");
    }

    // ── Именованная задача для наглядного логирования ──
    static class NamedTask implements Runnable {
        private final String name;
        private final int sleepMs;

        NamedTask(String name, int sleepMs) {
            this.name = name;
            this.sleepMs = sleepMs;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public String toString() {
            return name + "(sleep=" + sleepMs + "ms)";
        }
    }

    // ── Бенчмарк ──
    private static void benchmark() throws Exception {
        Logger log = Logger.getLogger("Benchmark");

        final int TASKS = 5000;
        final int TASK_DURATION_MS = 1;

        // Отключаем логи на время бенчмарка
        Logger poolLogger = Logger.getLogger("");
        Level originalLevel = poolLogger.getLevel();
        poolLogger.setLevel(Level.OFF);

        // ── Пул ──
        // Увеличиваем очередь, чтобы все задачи поместились
        CustomThreadPool custom = new CustomThreadPool(
                4, 8, 5, TimeUnit.SECONDS,
                1000, 1,   // queueSize=1000 (8 очередей × 1000 = 8000 слотов > 5000)
                new RoundRobinDistributor(),
                (task, poolName) -> { /* discard */ },
                "BenchCustom"
        );

        CountDownLatch latch1 = new CountDownLatch(TASKS);
        long start = System.nanoTime();
        for (int i = 0; i < TASKS; i++) {
            custom.execute(() -> {
                doWork(TASK_DURATION_MS);
                latch1.countDown();
            });
        }
        latch1.await(60, TimeUnit.SECONDS);
        long customTime = System.nanoTime() - start;
        custom.shutdown();
        custom.awaitTermination(30, TimeUnit.SECONDS);

        // ── Стандартный ThreadPoolExecutor ──
        ThreadPoolExecutor standard = new ThreadPoolExecutor(
                4, 8, 5, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(5000),  // одна очередь на 5000
                new ThreadPoolExecutor.DiscardPolicy()
        );

        CountDownLatch latch2 = new CountDownLatch(TASKS);
        start = System.nanoTime();
        for (int i = 0; i < TASKS; i++) {
            standard.execute(() -> {
                doWork(TASK_DURATION_MS);
                latch2.countDown();
            });
        }
        latch2.await(60, TimeUnit.SECONDS);
        long standardTime = System.nanoTime() - start;
        standard.shutdown();
        standard.awaitTermination(30, TimeUnit.SECONDS);

        // Включаем логи обратно
        poolLogger.setLevel(originalLevel);

        log.info("──────────────────────────────────────────");
        log.info("Tasks submitted: " + TASKS);
        log.info("Custom pool:   " + (customTime / 1_000_000) + " ms");
        log.info("Standard pool: " + (standardTime / 1_000_000) + " ms");
        log.info("──────────────────────────────────────────");
    }

    private static void doWork(int ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ── Настройка формата логов ──

    private static void setupLogging() {
        Logger root = Logger.getLogger("");
        for (Handler h : root.getHandlers()) root.removeHandler(h);

        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL);
        handler.setFormatter(new SimpleFormatter() {
            @Override
            public String format(LogRecord record) {
                return String.format("[%1$tT.%1$tL] %2$s%n",
                        record.getMillis(), record.getMessage());
            }
        });
        root.addHandler(handler);
        root.setLevel(Level.ALL);
    }
}
