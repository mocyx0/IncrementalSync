package org.pangolin.xuzhe.pipeline;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.pangolin.xuzhe.pipeline.Constants.BUFFER_SIZE;
import static org.pangolin.xuzhe.pipeline.Constants.POOL_SIZE;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ByteBufferPool {
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private static ByteBufferPool instance = new ByteBufferPool();
    public static ByteBufferPool getInstance() {
        return instance;
    }

    private BlockingQueue<ByteBuffer> pool = new ArrayBlockingQueue<ByteBuffer>(POOL_SIZE);

    private ByteBufferPool() {
        while(pool.remainingCapacity() > 0) {
            pool.offer(ByteBuffer.allocate(BUFFER_SIZE));
        }
    }

    public ByteBuffer get() throws InterruptedException {
        return pool.take();
    }

    public void put(ByteBuffer buffer) throws InterruptedException {
        buffer.clear();
        pool.put(buffer);
    }
}
