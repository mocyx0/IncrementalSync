package org.pangolin.xuzhe.test;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by ubuntu on 17-6-7.
 */
public class IOPerfTest {
    private static final int BUFFER_SIZE = 4 << 20;
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void reverseOrderReadByFileChannel(String fileName) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        FileChannel channel = file.getChannel();
        long size = channel.size();
        long pos = size;
        long cnt = 0;
        long begin = System.nanoTime();

        while (true) {
            pos -= buffer.capacity();
            buffer.clear();
            if (pos < 0) {
                buffer.limit((int) (buffer.capacity() + pos));
                pos = 0;
            }
            int n = channel.read(buffer, pos);
            cnt += n;
//            System.out.println("readed:" + n);
            if (pos == 0) break;

        }
        long end = System.nanoTime();
        logger.info("reverseOrderReadByFileChannel fileSize:{} read:{} elapsed time:{} ns", size, cnt, (end - begin));
    }

    public static void positiveOrderReadByFileChannel(String fileName) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        FileChannel channel = file.getChannel();
        long size = channel.size();
        long pos = size;
        long cnt = 0;
        long begin = System.nanoTime();

        while (true) {
            buffer.clear();
            int n = channel.read(buffer);
//            System.out.println("readed:" + n);
            if (n == -1) break;
            cnt += n;

        }
        long end = System.nanoTime();
        logger.info("positiveOrderReadByFileChannel fileSize:{} read:{} elapsed time:{} ns", size, cnt, (end - begin));
    }

    public static void main(String[] args) {
        try {
            positiveOrderReadByFileChannel("data/1.txt");
            System.gc();
            reverseOrderReadByFileChannel("data/1.txt");
        } catch (IOException e) {

        }
    }
}
