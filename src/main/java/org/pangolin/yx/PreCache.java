package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Created by yangxiao on 2017/6/10.
 */
public class PreCache {

    private static Logger logger = LoggerFactory.getLogger(Server.class);
    private static CountDownLatch latch;
    private static ConcurrentLinkedQueue<Task> fileTasks = new ConcurrentLinkedQueue<>();
    private static int BLOCK_SIZE = 1024 * 1024 * 128;

    private static class Task {
        String path;
        long off;
        long length;
    }

    private static class Worker implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    Task task = fileTasks.poll();
                    if (task == null) {
                        latch.countDown();
                        break;
                    } else {
                        RandomAccessFile raf = new RandomAccessFile(task.path, "r");
                        MappedByteBuffer mmap = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, task.off, task.length);
                        mmap.load();
                        raf.close();
                    }
                }
            } catch (Exception e) {
                logger.info("{}", e);
            }


        }
    }


    public static void precache(ArrayList<String> paths) throws Exception {
        logger.info("precache start");
        for (String s : paths) {
            File file = new File(s);
            if (file.exists()) {
                long len = file.length();
                for (int off = 0; off < len; off += BLOCK_SIZE) {
                    Task task = new Task();
                    task.path = s;
                    task.off = off;
                    task.length = Math.min(BLOCK_SIZE, len - off);
                    fileTasks.add(task);
                }
            }
        }

        int cpu = Runtime.getRuntime().availableProcessors();
        latch = new CountDownLatch(cpu);
        for (int i = 0; i < cpu; i++) {
            Thread th = new Thread(new Worker());
            th.start();
        }
        latch.await();
        //

        logger.info("precache end");
    }

}
