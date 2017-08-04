package org.pangolin.yx.test;

import org.pangolin.yx.zhengxu.ZXUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by yangxiao on 2017/7/9.
 */
public class UnsafeTest {


    public static void run() throws Exception {
        long t1 = System.currentTimeMillis();
        String path = "/root/ramfs/test";
        int readMax = 1024 * 1024 * 1024;
        RandomAccessFile raf = new RandomAccessFile(path, "rw");
        byte[] buff = new byte[1024*512];
        int pos = 0;
        int sum = 0;
        while (pos < readMax) {
            int len = raf.read(buff);
            pos += len;
            for (int i = 0; i < len; i++) {
                sum += buff[i];
            }
        }
        System.out.println(sum);
        long t2 = System.currentTimeMillis();
        System.out.println(t2 - t1);
    }

    public static void runUnsafe() throws Exception {
        System.out.println("runUnsafe");
        ZXUtil.init();
        Unsafe unsafe = ZXUtil.unsafe;
        long t1 = System.currentTimeMillis();
        String path = "/root/ramfs/test";
        int readMax = 1024 * 1024 * 1024;
        RandomAccessFile raf = new RandomAccessFile(path, "rw");
        FileChannel channel = raf.getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024*512);
        DirectBuffer direct = (DirectBuffer) buffer;
        long add = direct.address();
        int pos = 0;
        int sum = 0;
        while (pos < readMax) {
            buffer.clear();
            int len = channel.read(buffer);
            pos += len;
            for (int i = 0; i < len; i++) {
                sum += unsafe.getByte(add + i);
            }
        }
        System.out.println(sum);
        long t2 = System.currentTimeMillis();
        System.out.println(t2 - t1);
    }

    public static void main(String[] args) {
        try {
            runUnsafe();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
