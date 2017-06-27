package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Server;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Created by yangxiao on 2017/6/10.
 */
public class PreCache {

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
                MLog.info(e.toString());
            }


        }
    }

    private static class PrecacheInner implements Runnable {

        ArrayList<String> paths;

        PrecacheInner(ArrayList<String> paths) {
            this.paths = paths;
        }

        @Override
        public void run() {
            try {
                MLog.info("precache start");
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
                int thread = Config.PRECACHE_THREAD;
                latch = new CountDownLatch(thread);
                for (int i = 0; i < thread; i++) {
                    Thread th = new Thread(new Worker());
                    th.start();
                }
                latch.await();
                synchronized (PreCache.class) {
                    PreCache.class.notifyAll();
                }
                //
                MLog.info("precache end");
            } catch (Exception e) {
                MLog.info(e.toString());
                System.exit(0);
            }

        }
    }

    public static void precache(ArrayList<String> paths) throws Exception {
        Thread th = new Thread(new PrecacheInner(paths));
        th.start();
    }

}
