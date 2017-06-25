package org.pangolin.yx;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangxiao on 2017/6/20.
 */
public class ReadBufferPoll {
    private static int FILE_BLOCK_SIZE = 1024 * 1024*4;//
    private static BlockingQueue<byte[]> bufferPool = new LinkedBlockingQueue<>(Config.READ_POOL_SIZE);


    public static void init() throws Exception {
        for (int i = 0; i < Config.READ_POOL_SIZE; i++) {
            bufferPool.put(new byte[FILE_BLOCK_SIZE]);
        }
    }

    public static byte[] allocateReadBuff() throws Exception {
        byte[] re= bufferPool.take();
        return re;
/*
        byte[] buff = bufferPool.poll();
        if (buff == null) {
            buff = new byte[FILE_BLOCK_SIZE];
        }
        return buff;
        */
        //return new byte[FILE_BLOCK_SIZE];
    }

    public static void freeReadBuff(byte[] buff) throws Exception {
        bufferPool.put(buff);
    }

    public static int size() {
        return bufferPool.size();
    }
}
