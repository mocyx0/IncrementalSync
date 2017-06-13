package org.pangolin.xuzhe.test;

import com.alibaba.middleware.race.sync.Server;
import org.pangolin.yx.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.pangolin.xuzhe.test.Constants.WORKER_NUM;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ReadingThread extends Thread {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    String[] fileNameArray;
    Worker[] workers;

    public ReadingThread(String[] fileNameArray) {
        super("ReadingThread");
        this.fileNameArray = fileNameArray;
        workers = new Worker[WORKER_NUM];
        for (int i = 0; i < WORKER_NUM; i++) {
            workers[i] = new Worker();
        }
        for (int i = 0; i < WORKER_NUM; i++) {
            workers[i].start();
        }
    }

    @Override
    public void run() {
        ByteBufferPool pool = ByteBufferPool.getInstance();
        int workerIndex = 0;
        FileInputStream fis = null;
        try {
            for (String fileName : fileNameArray) {
                int fileNo = Integer.parseInt(fileName.substring(fileName.lastIndexOf("/") + 1, fileName.length() - 4));
                File f = new File(fileName);
                if (!f.exists()) {
                    logger.info("file: {} not exist", fileName);
                    continue;
                }
                fis = new FileInputStream(f);
                FileChannel channel = fis.getChannel();
                logger.info("read filename:{}", fileName);
                while (true) {
                    ByteBuffer buffer = pool.get();
                    long pos = channel.position();
                    int n = channel.read(buffer);
                    if (n == -1) {
                        pool.put(buffer);
                        break;
                    }
                    buffer.limit(n);
                    int limit = buffer.position();
                    buffer.position(buffer.limit() - 1);
                    while (buffer.get() != (byte) '\n') {
                        buffer.position(buffer.position() - 2);
                    }
                    channel.position(channel.position() - (limit - buffer.position()));
                    buffer.flip();
                    int w = workerIndex % WORKER_NUM;
//                    pool.put(buffer);
                    workers[w].appendBuffer(buffer, pos, fileNo);
                    ++workerIndex;
                }
                channel.close();
                fis.close();
                logger.info("read file: {} done!", fileName);
            }

            logger.info("Reading Done!");
            for (Worker worker : workers) {
                worker.appendBuffer(Worker.EMPTY_BUFFER, 0, 0);
            }
            for (Worker worker : workers) {
                worker.join();
            }
            Map<Integer, Map<String, AtomicLong>> tableLogCountMap = new HashMap<>();
            Map<Integer, Map<String, AtomicLong>> opCountMap = new HashMap<>();
            Map<Integer, AtomicLong> lineCountMap = new HashMap<>();
            for (int i = 1; i <= fileNameArray.length; i++) {
                tableLogCountMap.put(i, new HashMap<String, AtomicLong>());
                opCountMap.put(i, new HashMap<String, AtomicLong>());
                lineCountMap.put(i, new AtomicLong(0));
            }
            for (Worker worker : workers) {
                for (int i = 1; i <= fileNameArray.length; i++) {
                    Map<String, AtomicLong> map = worker.tableLogCountMap.get(i);
                    if (map != null) {
                        for (Map.Entry<String, AtomicLong> entry : map.entrySet()) {
                            if (tableLogCountMap.get(i).get(entry.getKey()) == null) {
                                tableLogCountMap.get(i).put(entry.getKey(), new AtomicLong(entry.getValue().longValue()));
                            } else {
                                tableLogCountMap.get(i).get(entry.getKey()).addAndGet(entry.getValue().longValue());
                            }
                        }
                    }
                    map = worker.opCountMap.get(i);
                    if (map != null) {
                        for (Map.Entry<String, AtomicLong> entry : map.entrySet()) {
                            if (opCountMap.get(i).get(entry.getKey()) == null) {
                                opCountMap.get(i).put(entry.getKey(), new AtomicLong(entry.getValue().longValue()));
                            } else {
                                opCountMap.get(i).get(entry.getKey()).addAndGet(entry.getValue().longValue());
                            }
                        }
                    }
                    AtomicLong lineCount = worker.lineCountMap.get(i);
                    if (lineCount != null) {
                        lineCountMap.get(i).addAndGet(lineCount.longValue());
                    }
                }
            }
            logger.info("每个表的log行数:{}", tableLogCountMap);
            logger.info("每种操作的数量:{}", opCountMap);
            logger.info("每个文件的行数：{}", lineCountMap);
        } catch (IOException e) {
            logger.info("{}", e);
        } catch (InterruptedException e) {
            logger.info("{}", e);
        } catch (Exception e) {
            logger.info("{}", e);
        }
    }


    public static void main(String[] args) throws InterruptedException {
        Config.init();
        String[] fileNameArray = new String[1];
        for(int i = 0; i < fileNameArray.length; i++) {
            fileNameArray[i] = Config.DATA_HOME + "/" + (i+1) + ".txt";
        }
        Long time1 = System.currentTimeMillis();
        ReadingThread readingThread = new ReadingThread(fileNameArray);
        readingThread.start();
        readingThread.join();
        Long time2 = System.currentTimeMillis();
        //  readingThread.sleep(3000);
        logger.info("从pagecache读时，需要花费的时间：{} ms", time2 - time1);
    }
}
