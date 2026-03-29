package pool;

import java.util.logging.Logger;

/**
 * «CallerRuns» — задача выполняется в потоке вызывающего.
 *
 * Почему выбран этот подход:
 * ─────────────────────────
 * 1. Обеспечивает back-pressure: вызывающий поток блокируется на время
 *    выполнения задачи, что автоматически замедляет темп поступления
 *    новых задач и даёт пулу время «разгрести» очередь.
 * 2. Ни одна задача не теряется — в отличие от DiscardPolicy / AbortPolicy.
 * 3. Не требует дополнительной инфраструктуры (retry-очередей, DLQ и т.п.).
 *
 * Недостатки:
 * ───────────
 * 1. Если вызывающий поток — это, например, acceptor-поток Netty/NIO,
 *    его блокировка приведёт к тому, что новые соединения перестанут
 *    приниматься на всё время выполнения задачи.
 * 2. Нарушается предсказуемость латентности: задача выполняется не в
 *    контролируемом worker-потоке, а в произвольном контексте.
 * 3. При очень долгих задачах back-pressure может «пробить» всю цепочку
 *    вызовов вплоть до клиента, что не всегда допустимо.
 */
public class CallerRunsOnRejectPolicy implements RejectionPolicy {

    private static final Logger log = Logger.getLogger(CallerRunsOnRejectPolicy.class.getName());

    @Override
    public void reject(Runnable task, String poolName) {
        log.warning("[Rejected] Task " + task + " was rejected due to overload in pool '"
                + poolName + "'. Executing in caller thread: " + Thread.currentThread().getName());
        // Выполняем задачу прямо в потоке вызывающего
        task.run();
    }
}
