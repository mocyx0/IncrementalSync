package org.pangolin.yx;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangxiao on 2017/6/20.
 */
public class ReadBufferPoll {
    private static int FILE_BLOCK_SIZE = 1024 * 1024 * 4;//
    private static final int BUFFER_COUNT = 12;
    private static BlockingQueue<byte[]> bufferPool = new LinkedBlockingQueue<>(BUFFER_COUNT);


    public static void init() throws Exception {
        for (int i = 0; i < BUFFER_COUNT; i++) {
            bufferPool.put(new byte[FILE_BLOCK_SIZE]);
        }
    }

    public static byte[] allocateReadBuff() throws Exception {
        return bufferPool.take();
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
