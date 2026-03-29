package pool;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Least-Loaded: задача отправляется в очередь с наименьшим числом элементов.
 */
public class LeastLoadedDistributor implements TaskDistributor {

    @Override
    public int selectQueue(List<BlockingQueue<Runnable>> queues) {
        int bestIdx = -1;
        int bestSize = Integer.MAX_VALUE;
        for (int i = 0; i < queues.size(); i++) {
            BlockingQueue<Runnable> q = queues.get(i);
            int sz = q.size();
            if (q.remainingCapacity() > 0 && sz < bestSize) {
                bestSize = sz;
                bestIdx = i;
            }
        }
        return bestIdx; // -1 если все переполнены
    }
}
