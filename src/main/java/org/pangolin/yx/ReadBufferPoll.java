package org.pangolin.yx;

import java.util.concurrent.*;

/**
 * Created by yangxiao on 2017/6/20.
 */
public class ReadBufferPoll {
    private static BlockingDeque<byte[]> bufferPool = new LinkedBlockingDeque<>(Config.READ_POOL_SIZE);


    public static void init() throws Exception {
        for (int i = 0; i < Config.READ_POOL_SIZE; i++) {
            bufferPool.put(new byte[Config.READ_BUFFER_SIZE]);
        }
    }

    public static byte[] allocateReadBuff() throws Exception {
        byte[] re = bufferPool.takeFirst();
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
        bufferPool.putFirst(buff);
    }

    public static int size() {
        return bufferPool.size();
    }
}
