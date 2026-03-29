package pool;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Стратегия распределения задач по очередям воркеров.
 */
public interface TaskDistributor {
    /**
     * Выбирает индекс очереди, в которую следует поместить задачу.
     *
     * @param queues список очередей воркеров
     * @return индекс выбранной очереди, или -1 если все переполнены
     */
    int selectQueue(List<BlockingQueue<Runnable>> queues);
}
