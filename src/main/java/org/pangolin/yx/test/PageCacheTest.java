package org.pangolin.yx.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/15.
 */
public class PageCacheTest {

    static String path = "D:/tmp/pagecache/1.data";
    private static int mmapSize = 1 * 1024 * 1024 * 1024;

    public static void testRafWrite() throws Exception {
        long totalSize = 2L * 1024 * 1024 * 1024;
        int blockSize = 4 * 1024;
        byte[] data = new byte[blockSize];

        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
        long t1 = System.currentTimeMillis();
        RandomAccessFile raf = new RandomAccessFile(path, "rw");
        long pos = 0;
        while (pos < totalSize) {
            raf.write(data);
            pos += data.length;
        }
        long t2 = System.currentTimeMillis();
        float speed = (totalSize / (1024 * 1024)) / ((t2 - t1) / 1000);
        System.out.println(String.format("write %d time %d speed %f", totalSize, t2 - t1, speed));
    }

    public static void testMmapWrite() throws Exception {
        long totalSize = 6L * 1024 * 1024 * 1024;

        int blockSize = 4 * 1024;
        byte[] data = new byte[blockSize];

        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
        long t1 = System.currentTimeMillis();
        RandomAccessFile raf = new RandomAccessFile(path, "rw");

        ArrayList<MappedByteBuffer> mmaps = new ArrayList<>();

        long mapLen = 0;
        while (mapLen < totalSize) {
            int len = (int) Math.min(mmapSize, totalSize - mapLen);
            MappedByteBuffer mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, mapLen, len);
            mmaps.add(mmap);
            mapLen += len;
        }

        long pos = 0;
        while (pos < totalSize) {
            int index = (int) (pos / mmapSize);
            mmaps.get(index).put(data, 0, data.length);
            pos += data.length;
        }
        long t2 = System.currentTimeMillis();
        float speed = (totalSize / (1024 * 1024)) / ((t2 - t1) / 1000);
        raf.close();
        System.out.println(String.format("write %d time %d speed %f", totalSize, t2 - t1, speed));
    }

    public static ArrayList<MappedByteBuffer> mapFile(RandomAccessFile raf, FileChannel.MapMode mode) throws Exception {
        ArrayList<MappedByteBuffer> mmaps = new ArrayList<>();
        long totalSize = raf.length();
        long mapLen = 0;
        while (mapLen < totalSize) {
            int len = (int) Math.min(mmapSize, totalSize - mapLen);
            MappedByteBuffer mmap = raf.getChannel().map(mode, mapLen, len);
            mmaps.add(mmap);
            mapLen += len;
        }
        return mmaps;
    }


    public static void testRafRead() throws Exception {
        File file = new File(path);
        int blockSize = 4 * 1024;
        byte[] data = new byte[blockSize];
        RandomAccessFile raf = new RandomAccessFile(path, "r");
        long pos = 0;
        long t1 = System.currentTimeMillis();
        while (pos < raf.length()) {
            raf.read(data);
            pos += data.length;
        }
        long t2 = System.currentTimeMillis();
        long totalSize = raf.length();
        float speed = (totalSize / (1024 * 1024)) / ((t2 - t1) / 1000);
        System.out.println(String.format("write %d time %d speed %f", totalSize, t2 - t1, speed));
    }

    public static void testMmapRead() throws Exception {
        File file = new File(path);
        int blockSize = 4 * 1024;
        byte[] data = new byte[blockSize];
        RandomAccessFile raf = new RandomAccessFile(path, "r");

        ArrayList<MappedByteBuffer> mmaps = mapFile(raf, FileChannel.MapMode.READ_ONLY);

        long pos = 0;
        long t1 = System.currentTimeMillis();
        while (pos < raf.length()) {
            int index = (int) (pos / mmapSize);
            ByteBuffer bf = mmaps.get(index);
            int oldpos = bf.position();
            bf.get(data);
            pos += bf.position() - oldpos;
        }

        long t2 = System.currentTimeMillis();
        long totalSize = raf.length();
        float speed = (totalSize / (1024 * 1024)) / ((t2 - t1) / 1000);
        System.out.println(String.format("write %d time %d speed %f", totalSize, t2 - t1, speed));
    }

    public static void main(String[] args) {
        try {
            testMmapRead();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
