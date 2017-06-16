package org.pangolin.yx;


import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/6/4.
 */
public class Util {

    public static ArrayList<String> logFiles(String dir) {
        ArrayList<String> files = new ArrayList<>();
        for (int i = 1; i < 100; i++) {
            String filepath = dir + "/" + i + ".txt";
            File f = new File(filepath);
            if (f.exists()) {
                files.add(filepath);
            } else {
                break;
            }
        }
        return files;
    }

    public static ArrayList<MappedByteBuffer> mapFile(RandomAccessFile raf, FileChannel.MapMode mode) throws Exception {
        int mmapSize = Integer.MAX_VALUE;
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

}


