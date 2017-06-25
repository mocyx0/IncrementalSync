package org.pangolin.yx.test;

import org.pangolin.yx.Config;
import org.pangolin.yx.ReadBufferPoll;
import org.pangolin.yx.Util;
import sun.nio.ch.DirectBuffer;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/25.
 */
public class DirectBuffRead {
    private static void run() throws Exception {
        Config.init();
        ArrayList<String> files = Util.logFiles(Config.DATA_HOME);
        ByteBuffer buffer = ByteBuffer.allocate(4 * 1024 * 1024);
        for (int i = 0; i < files.size(); i++) {
            String path = files.get(i);
            RandomAccessFile raf = new RandomAccessFile(path, "r");
            FileChannel channel = raf.getChannel();
            long fileLen = raf.length();
            long pos = 0;
            while (pos < fileLen) {
                pos += channel.read(buffer);
                buffer.flip();
                buffer.clear();
            }
        }
    }

    public static void main(String[] args) {
        try {
            long t1 = System.currentTimeMillis();
            run();
            long t2 = System.currentTimeMillis();
            System.out.print(t2 - t1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
