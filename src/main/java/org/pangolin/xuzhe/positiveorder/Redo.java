package org.pangolin.xuzhe.positiveorder;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    static Logger logger = LoggerFactory.getLogger(Server.class);
//    public MyLong2IntHashMap pkMap = new MyLong2IntWithBitIndexHashMap(32-Integer.numberOfLeadingZeros(10000000/REDO_NUM), 0.99f);
//    private byte[] dataSrc;
    private Parser[] parser;
    int redoId;
    private DataStore dataStore;
    private int beginId;
    private int endId;
    private HashMap<Integer, Redo.Record> map;
    public  Redo(int redoId, Parser[] parser){
        this.redoId = redoId;
        setName("Redo" + redoId);
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
//        return this.dataStore.getRecord(pk, out, pos);
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
//        long interval = (endPk - beginPk + 1)/REDO_NUM;
//        long currentBeginPk = interval * (redoNum - 1) + beginPk;
//        long currentEndPk = currentBeginPk + interval - 1;
//        if(redoNum == REDO_NUM)
//            currentEndPk = endPk;
//        if(currentBeginPk == beginPk)
//            currentBeginPk += 1;
//        if(currentEndPk == endPk)
//            currentEndPk -= 1;
//         System.out.println(Thread.currentThread().getName() + ":" + currentBeginPk + " " + currentEndPk);

        long count = 0;
        LogIndex logIndex;
        try {
            while (true) {
                int parserNum = (int) (count % PARSER_NUM);
                //获取对应parser对象的logIndex
                logIndex = parser[parserNum].getLogIndexQueueHeaderByRedoId(redoId);
                if (logIndex == LogIndex.EMPTY_LOG_INDEX) {
                    break;
                }
//                dataSrc = logIndex.getByteBuffer().array();
//                System.out.println(new String(dataSrc, 0, 100));
                try {
                    dataStore.process(logIndex);
//                    long[] oldPKs = logIndex.getOldPks();
//                    for (int i = 0; i < logIndex.getLogSize(); i++) {
//                        int logType = logIndex.getLogType(i);
//                        if (logType == 'I') {
//                            long newPk =logIndex.getNewPk(i);
//
//                            if(newPk % REDO_NUM == this.redoId) {
//                                insertResualt(pkMap, logIndex, i, newPk);
//                            }
//                        } else if (logType == 'U') {
//                            updateResualt(pkMap, logIndex, i, oldPKs[i]);
//                        } else {
//                            deleteResualt(pkMap, logIndex, oldPKs[i]);
//                        }
//                    }

//                    System.out.println("LogIndex Done!" + count);
                } finally {
                    count++;
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
            logger.info( off + "bytes, Done," + (time2-time1) + " ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//
//    private void deleteResualt(MyLong2IntHashMap pkMap, LogIndex logIndex, long oldPk) throws InterruptedException {
//        int indexInStore = pkMap.remove(oldPk);
//        if(indexInStore == -1) {
//
//        } else {
//            dataStore.deleteRecord(indexInStore);
//        }
//    }
//
//    private void updateResualt(MyLong2IntHashMap pkMap, LogIndex logIndex, int index, long oldPk) throws InterruptedException {
//        int recordIndexInStore = pkMap.get(oldPk);
//        if (recordIndexInStore == -1) {
//            return;
//        }
//        long newPk = logIndex.getNewPk(index);
//        int[] newValues = logIndex.getColumnNewValues(index);
//        int[] names = logIndex.getHashColumnName(index);
//        int[] valueLens = logIndex.getColumnValueLens(index);
//        int columnSize = logIndex.getColumnSize(index);
//        for (int i = 0; i < columnSize; i++) {
//            int columnIndex = names[i];
//            int columnPos = newValues[i];
//            int columnLen = valueLens[i];
//            dataStore.updateRecord(recordIndexInStore, columnIndex, dataSrc, columnPos, columnLen);
////            if (columnLen > columnByteBuffer.capacity()) {
//                //重新申请一个更大长度的byteBuffer
////                columnByteBuffer = ByteBuffer.allocate(columnLen);
////                allocatedBufferCount.incrementAndGet();
////            }
////            byteBuffer.position(columnPos);
////            System.arraycopy(byteBuffer.array(), columnPos, columnByteBuffer.array(), 0, columnLen);
////            columnByteBuffer.limit(columnLen);
//        }
//        if (oldPk != newPk) {
//            pkMap.remove(oldPk);
////            record.setPk(newPk);
//            pkMap.put(newPk, recordIndexInStore);
//        }
//    }
//
//    private void insertResualt(MyLong2IntHashMap pkMap, LogIndex logIndex, int index, long newPk) throws InterruptedException {
////        if (newPk <= beginPk || newPk >= endPk)
////            return;
//        //      int logSize = logIndex.getLogSize();
//        //每个pk对应到不同的线程
//        int columnSize = logIndex.getColumnSize(index);
//        int indexInStore = dataStore.createRecord();
////        Record record = new Record(newPk, columnSize);
//        for (int i = 0; i < columnSize; i++) {
//            int columnIndex = logIndex.getHashColumnName(index)[i];
//            int columnPos = logIndex.getColumnNewValues(index)[i];
//            int columnLen = logIndex.getColumnValueLens(index)[i];
//            dataStore.updateRecord(indexInStore, columnIndex, dataSrc, columnPos, columnLen);
////            record.setColumnValue(columnIndex, dataSrc, columnPos, columnLen);
////                allocatedBufferCount.incrementAndGet();
////                ByteBuffer columnByteBuffer = ByteBuffer.allocate(8);
//
////                System.arraycopy(byteBuffer.array(), columnPos, columnByteBuffer.array(), 0, columnLen);
//
////                columnByteBuffer.limit(columnLen);
////                String sf = new String(columnByteBuffer.array());
////                record.getColumnValue().add(columnByteBuffer);
//
//        }
//        pkMap.put(newPk, indexInStore);
//    }
//

}
