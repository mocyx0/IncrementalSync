package org.pangolin.xuzhe.positiveorder;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.pangolin.xuzhe.positiveorder.Constants.BUFFER_SIZE;
import static org.pangolin.xuzhe.positiveorder.Constants.POOL_SIZE;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ReadBufferPool {
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private static ReadBufferPool instance = new ReadBufferPool();
    public static ReadBufferPool getInstance() {
        return instance;
    }

    private BlockingQueue<ByteBuffer> pool = new ArrayBlockingQueue<ByteBuffer>(POOL_SIZE);

    private ReadBufferPool() {
        while(pool.remainingCapacity() > 0) {
            pool.offer(ByteBuffer.allocate(BUFFER_SIZE));
        }
    }

    public ByteBuffer get() throws InterruptedException {
        System.out.println("ReadBufferPool get:" + pool.size());
        return pool.take();
    }

    public void put(ByteBuffer buffer) throws InterruptedException {
        buffer.clear();
        System.out.println("ReadBufferPool put:" + pool.size());
        pool.put(buffer);
    }
}
