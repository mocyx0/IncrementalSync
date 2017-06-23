package org.pangolin.yx.zhengxu;

import org.pangolin.yx.Config;
import org.pangolin.yx.ReadBufferPoll;
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
    LinkedBlockingQueue<LogBlock> queue;
    Logger logger;
    DataStorage dataStorage;
    private int logCount = 0;
    private int index = 0;
    private int reBuilderCount;

    public Rebuilder(LinkedBlockingQueue<LogBlock> queue, CountDownLatch latch, TableInfo tableInfo, int index, int reBuilderCount) {
        this.queue = queue;
        this.latch = latch;
        this.index = index;
        this.reBuilderCount = reBuilderCount;
        logger = Config.serverLogger;
        dataStorage = new DataStoragePlain(tableInfo);
        //logger.info( String.format(String.format("rebuilder %d %d ",index,reBuilderCount)));
    }

    DataStorage getDataStorage() {
        return dataStorage;
    }

    @Override
    public void run() {
        try {
            while (true) {
                LogBlock logBlock = queue.take();
                if (logBlock == LogBlock.EMPTY) {
                    break;
                } else {
                    if (DO_REBUILD) {
                        for (LogRecord log : logBlock.logRecordsArr.get(index)) {
                            //logCount++;
                            dataStorage.doLog(log, logBlock.fileBlock.buffer);
                        }
                        //free buffer
                        if (logBlock.ref.decrementAndGet() == 0) {
                            ReadBufferPoll.freeReadBuff(logBlock.fileBlock.buffer);
                        }
                    } else {
                        if (logBlock.ref.decrementAndGet() == 0) {
                            ReadBufferPoll.freeReadBuff(logBlock.fileBlock.buffer);
                        }
                    }
                }
            }
            //logger.info(String.format("rebuild handle log:%d", logCount));
            latch.countDown();
        } catch (Exception e) {
            logger.info("{}", e);
            System.exit(0);
        }
    }
}
