package org.pangolin.yx.zhengxu;

import org.pangolin.yx.Config;
import org.pangolin.yx.MLog;
import org.pangolin.yx.ReadBufferPoll;
import sun.misc.Unsafe;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangxiao on 2017/6/17.
 */
public class Rebuilder implements Runnable {
    //啥也不做 用于测试
    private final boolean DO_REBUILD = true;
    private final CountDownLatch latch;
    private final BlockingQueue<LogBlock> queue;
    private DataStorage dataStorage;
    private final int index;
    private int reBuilderCount;

    public Rebuilder(BlockingQueue<LogBlock> queue, CountDownLatch latch, TableInfo tableInfo, int index, int reBuilderCount) {
        this.queue = queue;
        this.latch = latch;
        this.index = index;
        this.reBuilderCount = reBuilderCount;
        //logger.info( String.format(String.format("rebuilder %d %d ",index,reBuilderCount)));
        unsafe = ZXUtil.unsafe;
    }

    DataStorage getDataStorage() {
        return dataStorage;
    }

    Unsafe unsafe;

    @Override
    public void run() {
        try {
            dataStorage = new DataStorageTwoLevel(GlobalData.tableInfo);
            while (true) {
                LogBlock logBlock = queue.take();

                if (logBlock == LogBlock.EMPTY) {
                    break;
                } else {
                    if (DO_REBUILD) {
                        for (int i = 0; i < logBlock.length; i++) {
                            //if (logBlock.redoer[i] == index) {
                            if (unsafe.getByte(logBlock.redoer + i) == index) {
                                dataStorage.doLog(logBlock, null, i);
                            }
                        }
                        /*
                        LogBlockRebuilder logBlockRebuilder = logBlock.logBlockRebuilders[index];
                        for (int i = 0; i < logBlockRebuilder.length; i++) {
                            dataStorage.doLog(logBlock, null, logBlockRebuilder.poss[i]);
                        }
                        */
                        /*
                        for (LogRecord log : logBlock.logRecordsArr.get(index)) {
                            //logCount++;
                            dataStorage.doLog(log, logBlock.fileBlock.buffer);
                        }
                        */
                        //free buffer
                        if (logBlock.ref.decrementAndGet() == 0) {
                            //ReadBufferPoll.freeReadBuff(logBlock.fileBlock.buffer);
                            LogBlock.free(logBlock);
                        }
                    } else {
                        if (logBlock.ref.decrementAndGet() == 0) {
                            //ReadBufferPoll.freeReadBuff(logBlock.fileBlock.buffer);
                            LogBlock.free(logBlock);

                        }
                    }
                }
            }
            //logger.info(String.format("rebuild handle log:%d", logCount));
            latch.countDown();
        } catch (Exception e) {
            MLog.info(e);
            System.exit(0);
        }
    }
}
