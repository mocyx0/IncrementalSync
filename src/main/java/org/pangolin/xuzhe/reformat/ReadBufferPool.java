package org.pangolin.xuzhe.reformat;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.pangolin.xuzhe.reformat.Constants.BUFFER_SIZE;
import static org.pangolin.xuzhe.reformat.Constants.READBUFFER_POOL_SIZE;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ReadBufferPool {
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private static ReadBufferPool instance = new ReadBufferPool();
    public static ReadBufferPool getInstance() {
        return instance;
    }

    private BlockingQueue<ByteBuffer> pool = new ArrayBlockingQueue<ByteBuffer>(READBUFFER_POOL_SIZE);

    private ReadBufferPool() {
        while(pool.remainingCapacity() > 0) {
            pool.offer(ByteBuffer.allocate(BUFFER_SIZE));
        }
    }

    public ByteBuffer get() throws InterruptedException {
//        System.out.println(Thread.currentThread().getName() + " ReadBufferPool get, remain:" + pool.size());
        ByteBuffer buffer =  pool.take();
//        System.out.println(Thread.currentThread().getName() + " get a buffer");
        return buffer;
    }

    public void put(ByteBuffer buffer) throws InterruptedException {
        buffer.clear();
//        System.out.println(Thread.currentThread().getName() + " ReadBufferPool put, remain:" + pool.size());
        pool.put(buffer);
//        System.out.println(Thread.currentThread().getName() + " put a buffer");
    }
}
