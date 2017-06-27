package org.pangolin.xuzhe.positiveorder;

import com.alibaba.middleware.race.sync.Server;
import org.pangolin.yx.MLog;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.pangolin.xuzhe.positiveorder.Constants.PARSER_NUM;
import static org.pangolin.xuzhe.positiveorder.Constants.REDO_NUM;
import static org.pangolin.xuzhe.positiveorder.ReadingThread.parserLatch;


/**
 * Created by 29146 on 2017/6/16.
 */
public class Redo extends Thread {
    private Parser[] parser;
    int redoId;
    private DataStore dataStore;
    private int beginId;
    private int endId;
    private HashMap<Integer, Redo.Record> map;
    public  Redo(int redoId, Parser[] parser){
        setName("Redo" + redoId);
        this.redoId = redoId;
        this.parser = parser;
    }

    public void setSearchRange(int begin, int end) {
        beginId = begin;
        endId = end;
    }

    public int getRecord(long pk, byte[] out, int pos) {
        if(this.dataStore.exist((int)pk)) {
            Redo.Record r = this.map.get((int)pk);
            System.arraycopy(r.data, 0, out, pos, r.len);
            return pos + r.len;
        } else {
            return -1;
        }
    }

    public static class Record {
        public final byte[] data;
        public final int len;
        public Record(byte[] data, int len) {
            this.data = data;
            this.len = len;
        }
    }

    @Override
    public void run() {
        try {
            parserLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Schema schema = Schema.getInstance();
        dataStore = new DataStore(schema.columnCount, this.redoId);
        long number = 0; // 控制遍历Parser的序号
        LogIndex logIndex;
        try {
            while (true) {
                int parserNum = (int) (number % PARSER_NUM);
                //获取对应parser对象的logIndex
                logIndex = parser[parserNum].getLogIndexQueueHeaderByRedoId(redoId);
                if (logIndex == LogIndex.EMPTY_LOG_INDEX) {
                    break;
                }
                try {
                    dataStore.process(logIndex);
                } finally {
                    number++;
                    logIndex.release();
                }
            }
            long time1 = System.currentTimeMillis();
            map = new HashMap<>(100_0000, 0.85f);
            int off = 0;
            byte[] buf = new byte[100];
            for(int i = beginId + 1; i < endId; i++ ) {
                int len = this.dataStore.getRecord(i, buf, 0);
                if(len != -1) {
                    map.put(i, new Redo.Record(buf, len));
                    off += len;
                    buf = new byte[100];
                }
            }
            long time2 = System.currentTimeMillis();
            MLog.info( off + "bytes, Done," + (time2-time1) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
