package org.pangolin.yx.zhengxu;

import org.pangolin.yx.Config;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangxiao on 2017/6/17.
 */
public class Rebuilder implements Runnable {
    //啥也不做 用于测试
    private boolean DO_REBUILD = true;

    CountDownLatch latch;
    LinkedBlockingQueue<ArrayList<LogRecord>> queue;
    Logger logger;
    DataStorage dataStorage;
    private int logCount = 0;

    public Rebuilder(LinkedBlockingQueue<ArrayList<LogRecord>> queue, CountDownLatch latch, TableInfo tableInfo) {
        this.queue = queue;
        this.latch = latch;
        logger = Config.serverLogger;
        dataStorage = new DataStorageHashMap(tableInfo);
    }

    DataStorage getDataStorage() {
        return dataStorage;
    }

    @Override
    public void run() {
        try {
            while (true) {
                ArrayList<LogRecord> logRecord = queue.take();
                if (logRecord.size() == 0) {
                    break;
                } else {
                    if (DO_REBUILD) {
                        for (LogRecord log : logRecord) {
                            logCount++;
                            dataStorage.doLog(log);
                        }
                    }
                }
            }
            logger.info(String.format("rebuild handle log:%d", logCount));
            latch.countDown();
        } catch (Exception e) {
            logger.info("{}", e);
            System.exit(0);
        }
    }
}
