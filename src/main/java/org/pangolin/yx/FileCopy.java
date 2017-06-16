package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Created by yangxiao on 2017/6/15.
 * 文件拷贝
 */
public class FileCopy {

    private static int BUFFER_SIZE = 4096;
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private static CountDownLatch latch;
    private static ConcurrentLinkedQueue<Task> tasks = new ConcurrentLinkedQueue<>();

    private static class Task {
        String from;
        String to;
    }

    private static class Worker implements Runnable {

        Worker() {
        }

        @Override
        public void run() {
            try {

                while (true) {
                    Task task = tasks.poll();
                    if (task == null) {
                        break;
                    } else {
                        String from = task.from;
                        String to = task.to;
                        RandomAccessFile raf = new RandomAccessFile(from, "r");
                        File newFile = new File(to);
                        if (newFile.exists()) {
                            newFile.delete();
                        }
                        RandomAccessFile newRaf = new RandomAccessFile(to, "rw");
                        MappedByteBuffer newMmap = newRaf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raf.length());
                        byte[] buffer = new byte[BUFFER_SIZE];
                        while (raf.getFilePointer() != raf.length()) {
                            int len = raf.read(buffer);
                            newMmap.put(buffer, 0, len);
                        }
                        raf.close();
                        newRaf.close();
                    }
                }


                latch.countDown();
            } catch (Exception e) {
                logger.info("{}", e);
                System.exit(0);
            }

        }
    }


    public static void copyFile(ArrayList<String> files, String toDir, boolean merge) throws Exception {
        long copySize = 0;
        long t1 = System.currentTimeMillis();
        if (merge) {
            String newPath = toDir + "/1.txt";
            RandomAccessFile newRaf = new RandomAccessFile(newPath, "rw");
            long pos = 0;
            for (String filePath : files) {
                RandomAccessFile raf = new RandomAccessFile(filePath, "r");
                MappedByteBuffer mmap = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
                MappedByteBuffer newMmap = newRaf.getChannel().map(FileChannel.MapMode.READ_WRITE, pos, raf.length());
                newMmap.put(mmap);
                pos += raf.length();
                raf.close();
            }
            newRaf.close();
        } else {
            int thcount = Config.CPU_COUNT;
            thcount = 1;//只允许单线程读取
            latch = new CountDownLatch(thcount);
            for (String path : files) {
                File file = new File(path);
                String newPath = toDir + "/" + file.getName();
                Task task = new Task();
                task.from = path;
                task.to = newPath;
                tasks.add(task);
                copySize += file.length();
            }

            for (int i = 0; i < thcount; i++) {
                Thread th = new Thread(new Worker());
                th.start();
            }
            latch.await();
        }
        long t2 = System.currentTimeMillis();
        float speed = (copySize / (1024 * 1024)) / ((t2 - t1) / 1000);
        logger.info(String.format("write %d time %d speed %f", copySize, t2 - t1, speed));
    }

    public static void main(String[] args) {
        try {
            ArrayList<String> files = Util.logFiles("D:/tmp/amimid/log10g");
            copyFile(files, "D:/tmp/pagecacheCopy", false);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
