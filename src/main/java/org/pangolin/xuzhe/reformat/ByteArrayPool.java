package org.pangolin.xuzhe.reformat;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.pangolin.xuzhe.reformat.Constants.*;

/**
 * Created by XuZhe on 2017/6/25.
 */
public class ByteArrayPool {
    public static final ByteArray EMPTY_ARRAY = new ByteArray(0);
    private static ByteArrayPool instance = new ByteArrayPool();
    public static ByteArrayPool getInstance() {
        return instance;
    }

    private BlockingQueue<ByteArray> pool = new ArrayBlockingQueue<ByteArray>(BYTEARRAY_POOL_SIZE);

    private ByteArrayPool() {
        while(pool.remainingCapacity() > 0) {
            pool.offer(new ByteArray(BYTEARRAY_SIZE));
        }
    }

    public ByteArray get() throws InterruptedException {
//        System.out.println("ByteArrayPool.get size:" + instance.pool.size());
        ByteArray buffer =  pool.take();
        return buffer;
    }

    public  void put(ByteArray buffer) throws InterruptedException {
//        System.out.println("ByteArrayPool.put");
        buffer.reset();
        pool.put(buffer);
    }

    public static class ByteArray {
        public final byte[] array;
        public int dataSize;
        public int no;
        private AtomicInteger refCount = new AtomicInteger(REDO_NUM);
        public ByteArray(int size) {
            array = new byte[size];
        }
        public synchronized void release() {
            if(refCount.decrementAndGet() == 0) {
                try {
//                    System.out.println("ByteArrayPool release, size:" + instance.pool.size());
                    this.reset();
                    instance.pool.put(this);
//                    System.out.println("BYTE ARRAY RELAESED");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        public void reset() {
            refCount.set(REDO_NUM);
            dataSize = 0;
        }
    }
}
