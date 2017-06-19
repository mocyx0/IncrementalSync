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
    public static AtomicInteger allocatedBufferCount = new AtomicInteger(0);
    //   private ByteBufferPool byteBufferPool = ByteBufferPool.getInstance();
    private Parser[] parser;
    int redoId;
    public  Redo(int redoId, Parser[] parser){
        this.redoId = redoId;
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
                        byte logType = logIndex.getLogType(i);
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

    private void deleteResualt(MyLong2ObjHashMap pkMap, LogIndex logIndex, long oldPk) throws InterruptedException {
        pkMap.remove(oldPk);
    }

    private void updateResualt(MyLong2ObjHashMap pkMap, LogIndex logIndex, int index, long oldPk) throws InterruptedException {
        Record record = pkMap.get(oldPk);
        if (record == null) {
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
            record.setPk(newPk);
            pkMap.put(newPk, record);
        }
    }

    private void insertResualt(MyLong2ObjHashMap pkMap, LogIndex logIndex, int index, long newPk) throws InterruptedException {
//        if (newPk <= beginPk || newPk >= endPk)
//            return;
        //      int logSize = logIndex.getLogSize();
        //每个pk对应到不同的线程
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
