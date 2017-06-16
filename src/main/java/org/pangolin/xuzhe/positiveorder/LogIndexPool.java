package org.pangolin.xuzhe.positiveorder;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.pangolin.xuzhe.positiveorder.Constants.LOGINDEXPOOL_SIZE;

/**
 * Created by ubuntu on 17-6-16.
 */
public class LogIndexPool {
    private static LogIndexPool instance;
    public static LogIndexPool getInstance() {
        if(instance == null) {
            throw new RuntimeException("请先设置columnCount");
        }
        return instance;
    }
    private static int columnCount;
    public static synchronized void setColumnCount(int columnCount) {
        if(instance != null) {
            throw new RuntimeException("columnCount被重复设置");
        }
        instance = new LogIndexPool(columnCount);
        LogIndexPool.columnCount = columnCount;
    }
    private BlockingQueue<LogIndex> pool = new ArrayBlockingQueue<LogIndex>(LOGINDEXPOOL_SIZE);

    private LogIndexPool(int columnCount) {
        while(pool.remainingCapacity() > 0) {
            pool.offer(new LogIndex(columnCount, this));
        }
    }

    public LogIndex get() throws InterruptedException {
        return pool.take();
    }

    public void put(LogIndex buffer) throws InterruptedException {
        buffer.reset();
        pool.put(buffer);
    }
}
