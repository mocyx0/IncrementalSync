package org.pangolin.xuzhe.test;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.pangolin.xuzhe.test.Constants.BUFFER_SIZE;
import static org.pangolin.xuzhe.test.Constants.POOL_SIZE;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ByteBufferPool {
    private static ByteBufferPool instance = new ByteBufferPool();
    public static ByteBufferPool getInstance() {
        return instance;
    }

    private BlockingQueue<ByteBuffer> pool = new ArrayBlockingQueue<ByteBuffer>(POOL_SIZE);

    private ByteBufferPool() {
        while(pool.remainingCapacity() > 0) {
            pool.offer(ByteBuffer.allocateDirect(BUFFER_SIZE));
        }
    }

    public ByteBuffer get() throws InterruptedException {
//        if(pool.size() < 2) return ByteBuffer.allocateDirect(BUFFER_SIZE);
        return pool.take();
    }

    public void put(ByteBuffer buffer) throws InterruptedException {
        buffer.clear();
//        if(pool.remainingCapacity() < 2) return;
        pool.put(buffer);
    }
}
