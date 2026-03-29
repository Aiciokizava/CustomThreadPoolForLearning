package pool;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round-Robin: задачи раздаются по очередям циклически.
 * Если выбранная очередь полна — пробуем следующую (полный круг).
 */
public class RoundRobinDistributor implements TaskDistributor {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int selectQueue(List<BlockingQueue<Runnable>> queues) {
        int size = queues.size();
        int start = counter.getAndIncrement() % size;
        // Пробуем начиная со следующей очереди; если полна — идём дальше
        for (int i = 0; i < size; i++) {
            int idx = (start + i) % size;
            if (queues.get(idx).remainingCapacity() > 0) {
                return idx;
            }
        }
        return -1; // все очереди переполнены
    }
}
