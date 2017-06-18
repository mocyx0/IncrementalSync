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
    public MyLong2ObjHashMap pkMap = new MyLong2ObjHashMap(10000000/REDO_NUM, 0.99f);
    private byte[] dataSrc;
    private long[] pkPos;
//    private List<Record> records = new ArrayList<>((1000*10000)/64);
    private static AtomicInteger redo = new AtomicInteger(0);
    public static AtomicInteger allocatedBufferCount = new AtomicInteger(0);
    //   private ByteBufferPool byteBufferPool = ByteBufferPool.getInstance();
    private Parser[] parser;
    int redoId = redo.incrementAndGet();
    public  Redo(Parser[] parser){
        setName("Redo" + redoId);
        this.parser = parser;
        pkPos = new long[(1000*10000)/64];
        for(int i = 0; i < pkPos.length; i++){
            pkPos[i] = 0;
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
        LogIndex logIndex = null;
        try {
            while (true) {
                int parserNum = (int) (count % PARSER_NUM);
                //获取对应parser对象的logIndex

                logIndex = parser[parserNum].getLogIndexQueueHeaderByRedoId(redoId);
                if (logIndex == LogIndex.EMPTY_LOG_INDEX) {
                    break;
                }


                dataSrc = logIndex.getByteBuffer().array();
                try {
                    for (int i = 0; i < logIndex.getLogSize(); i++) {
                        byte logType = logIndex.getLogType(i);
                        if (logType == 'I') {
                            insertResualt(pkMap, logIndex, i, redoId);
                        } else if (logType == 'U') {
                            updateResualt(pkMap, logIndex, i);
                        } else {
                            deleteResualt(pkMap, logIndex, i);
                        }
                    }

//                    System.out.println("LogIndex Done!" + count);
                    count++;
                } finally {
                    logIndex.release();
                }

            }
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public byte bitLongScan(long[] posPk, long pos){
        int blockBumber = (int)((pos - 1) / 64);
        byte curpos = (byte)((pos - 1) % 64);
        long value = posPk[blockBumber];
        byte bitValue = (byte) ((value >> curpos) & 1);
        return bitValue;
    }

    public void bitLongUpdate(long[] posPk, long pos, boolean bitFlag){
        int blockBumber = (int)((pos - 1) / 64);
        if(blockBumber > 4576316){
            System.out.println(blockBumber + " " + pos);
        }
        byte curpos = (byte)((pos - 1) % 64 );
        long value = posPk[blockBumber];
        if(bitFlag == true){
            value = value & ~(1 << curpos);
        }else{
            value = value | (1 << curpos);
        }
        posPk[blockBumber] = value;
    }

    private void deleteResualt(MyLong2ObjHashMap pkMap, LogIndex logIndex, int index) throws InterruptedException {
        long oldPk = logIndex.getOldPks()[index];
        pkMap.remove(oldPk);
    }

    private void updateResualt(MyLong2ObjHashMap pkMap, LogIndex logIndex, int index) throws InterruptedException {
        long oldPk = logIndex.getOldPks()[index];
        if (!pkMap.containsKey(oldPk)) {
            return;
        }
        Record record = pkMap.get(oldPk);
        long newPk = logIndex.getNewPk(index);
        short columnSize = logIndex.getColumnSize(index);
        for (int i = 0; i < columnSize; i++) {
            int columnIndex = logIndex.getHashColumnName(index)[i];
//            ByteBuffer columnByteBuffer = record.getColumnValue().get(pos);

            int columnPos = logIndex.getColumnNewValues(index)[i];
            int columnLen = logIndex.getColumnValueLens(index)[i];
            record.setColumnValue(columnIndex, dataSrc, columnPos, columnLen);
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
            pkMap.put(newPk, record);
        }
    }

    private void insertResualt(MyLong2ObjHashMap pkMap, LogIndex logIndex, int index, int redoNum) throws InterruptedException {
        long newPk = logIndex.getNewPk(index);
//        if (newPk <= beginPk || newPk >= endPk)
//            return;
        //      int logSize = logIndex.getLogSize();
        //每个pk对应到不同的线程
        if(newPk % REDO_NUM == (redoNum - 1)){
            short columnSize = logIndex.getColumnSize(index);
            Record record = new Record(newPk, columnSize);
            for (int i = 0; i < columnSize; i++) {
                int columnIndex = logIndex.getHashColumnName(index)[i];
                int columnPos = logIndex.getColumnNewValues(index)[i];
                int columnLen = logIndex.getColumnValueLens(index)[i];
                record.setColumnValue(columnIndex, dataSrc, columnPos, columnLen);
//                allocatedBufferCount.incrementAndGet();
//                ByteBuffer columnByteBuffer = ByteBuffer.allocate(8);

//                System.arraycopy(byteBuffer.array(), columnPos, columnByteBuffer.array(), 0, columnLen);

//                columnByteBuffer.limit(columnLen);
//                String sf = new String(columnByteBuffer.array());
//                record.getColumnValue().add(columnByteBuffer);

            }
            pkMap.put(newPk, record);
        }
    }


}
