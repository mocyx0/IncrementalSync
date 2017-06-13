package org.pangolin.xuzhe.pipeline;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.pangolin.xuzhe.pipeline.Constants.POOL_SIZE;
import static org.pangolin.xuzhe.pipeline.Constants.STRING_LIST_SIZE;

/**
 * Created by ubuntu on 17-6-13.
 */
public class StringArrayListPool {
    public static final ArrayList<String> EMPTY_STRING_LIST = new ArrayList<>(1);
    private static StringArrayListPool instance = new StringArrayListPool();
    public static StringArrayListPool getInstance() {
        return instance;
    }

    private BlockingQueue<ArrayList<String>> pool = new ArrayBlockingQueue<ArrayList<String>>(POOL_SIZE);

    private StringArrayListPool() {
        while(pool.remainingCapacity() > 0) {
            pool.offer(new ArrayList<String>(STRING_LIST_SIZE));
        }
    }

    public ArrayList<String> get() throws InterruptedException {
        return pool.take();
    }

    public void put(ArrayList<String> buffer) throws InterruptedException {
        buffer.clear();
        pool.put(buffer);
    }
}
