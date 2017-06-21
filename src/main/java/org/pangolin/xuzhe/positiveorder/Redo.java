package org.pangolin.xuzhe.positiveorder;

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
    public MyLong2IntHashMap pkMap = new MyLong2IntWithBitIndexHashMap(10000000/REDO_NUM, 0.99f);
    private byte[] dataSrc;
    private Parser[] parser;
    int redoId;
    private DataStore dataStore;
    public  Redo(int redoId, Parser[] parser){
        this.redoId = redoId;
        setName("Redo" + redoId);
        this.parser = parser;
    }

    public int getRecord(long pk, byte[] out, int pos) {
        int indexInStore = pkMap.get(pk);
        if(indexInStore == -1) {
            return -1;
        }
        return this.dataStore.getRecord(indexInStore, out, pos);
    }

    @Override
    public void run() {

        try {
            parserLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Schema schema = Schema.getInstance();
        dataStore = new DataStore(schema.columnCount);
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
                dataSrc = logIndex.getByteBuffer().array();
//                System.out.println(new String(dataSrc, 0, 100));
                try {
                    long[] oldPKs = logIndex.getOldPks();
                    for (int i = 0; i < logIndex.getLogSize(); i++) {
                        int logType = logIndex.getLogType(i);
                        if (logType == 'I') {
                            long newPk =logIndex.getNewPk(i);

                            if(newPk % REDO_NUM == this.redoId) {
                                insertResualt(pkMap, logIndex, i, newPk);
                            }
                        } else if (logType == 'U') {
                            updateResualt(pkMap, logIndex, i, oldPKs[i]);
                        } else {
                            deleteResualt(pkMap, logIndex, oldPKs[i]);
                        }
                    }

//                    System.out.println("LogIndex Done!" + count);
                } finally {
                    count++;
                    logIndex.release();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void deleteResualt(MyLong2IntHashMap pkMap, LogIndex logIndex, long oldPk) throws InterruptedException {
        int indexInStore = pkMap.remove(oldPk);
        if(indexInStore == -1) {

        } else {
            dataStore.deleteRecord(indexInStore);
        }
    }

    private void updateResualt(MyLong2IntHashMap pkMap, LogIndex logIndex, int index, long oldPk) throws InterruptedException {
        int recordIndexInStore = pkMap.get(oldPk);
        if (recordIndexInStore == -1) {
            return;
        }
        long newPk = logIndex.getNewPk(index);
        int[] newValues = logIndex.getColumnNewValues(index);
        int[] names = logIndex.getHashColumnName(index);
        short[] valueLens = logIndex.getColumnValueLens(index);
        short columnSize = logIndex.getColumnSize(index);
        for (int i = 0; i < columnSize; i++) {
            int columnIndex = names[i];
            int columnPos = newValues[i];
            int columnLen = valueLens[i];
            dataStore.updateRecord(recordIndexInStore, columnIndex, dataSrc, columnPos, columnLen);
//            if (columnLen > columnByteBuffer.capacity()) {
                //重新申请一个更大长度的byteBuffer
//                columnByteBuffer = ByteBuffer.allocate(columnLen);
//                allocatedBufferCount.incrementAndGet();
//            }
//            byteBuffer.position(columnPos);
//            System.arraycopy(byteBuffer.array(), columnPos, columnByteBuffer.array(), 0, columnLen);
//            columnByteBuffer.limit(columnLen);
        }
        if (oldPk != newPk) {
            pkMap.remove(oldPk);
//            record.setPk(newPk);
            pkMap.put(newPk, recordIndexInStore);
        }
    }

    private void insertResualt(MyLong2IntHashMap pkMap, LogIndex logIndex, int index, long newPk) throws InterruptedException {
//        if (newPk <= beginPk || newPk >= endPk)
//            return;
        //      int logSize = logIndex.getLogSize();
        //每个pk对应到不同的线程
        short columnSize = logIndex.getColumnSize(index);
        int indexInStore = dataStore.createRecord();
//        Record record = new Record(newPk, columnSize);
        for (int i = 0; i < columnSize; i++) {
            int columnIndex = logIndex.getHashColumnName(index)[i];
            int columnPos = logIndex.getColumnNewValues(index)[i];
            int columnLen = logIndex.getColumnValueLens(index)[i];
            dataStore.updateRecord(indexInStore, columnIndex, dataSrc, columnPos, columnLen);
//            record.setColumnValue(columnIndex, dataSrc, columnPos, columnLen);
//                allocatedBufferCount.incrementAndGet();
//                ByteBuffer columnByteBuffer = ByteBuffer.allocate(8);

//                System.arraycopy(byteBuffer.array(), columnPos, columnByteBuffer.array(), 0, columnLen);

//                columnByteBuffer.limit(columnLen);
//                String sf = new String(columnByteBuffer.array());
//                record.getColumnValue().add(columnByteBuffer);

        }
        pkMap.put(newPk, indexInStore);
    }


}
