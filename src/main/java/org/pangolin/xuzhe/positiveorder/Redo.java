package org.pangolin.xuzhe.positiveorder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.pangolin.xuzhe.positiveorder.Constants.*;


/**
 * Created by 29146 on 2017/6/16.
 */
public class Redo extends Thread {
    private Map<Long, Record> pkMap = new HashMap<>();
    private ByteBuffer byteBuffer;
    private long[] pkPos;
    private static AtomicInteger redo = new AtomicInteger(0);
    //   private ByteBufferPool byteBufferPool = ByteBufferPool.getInstance();
    private long beginPk;
    private long endPk;

    public  Redo(long beginPk, long endPk){
        this.beginPk = beginPk;
        this.endPk = endPk;
        pkPos = new long[(int)(this.endPk - this.endPk - 1)/64 + 1];
        for(int i = 0; i < pkPos.length; i++){
            pkPos[i] = 0;
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
        byte curpos = (byte)((pos - 1) % 64 );
        long value = posPk[blockBumber];
        if(bitFlag == true){
            value = value & ~(1 << curpos);
        }else{
            value = value | (1 << curpos);
        }
        posPk[blockBumber] = value;
    }

    @Override
    public void run() {
        int redoNum = redo.incrementAndGet();

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
        while(true){
            int parserNum = (int)(count % PARSER_NUM);
            //获取对应parser对象的logIndex
            LogIndex logIndex = null;
            byteBuffer = logIndex.getByteBuffer().duplicate();
            try {
                for (int i = 0; i < logIndex.getLogSize(); i++) {
                    byte logType = logIndex.getLogType(i);
                    if (logType == 'I') {
                        insertResualt(pkMap, logIndex, i, redoNum);
                    } else if (logType == 'U') {
                        updateResualt(pkMap, logIndex, i);
                    } else {
                        deleteResualt(pkMap, logIndex, i);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }



    }

    private void deleteResualt(Map<Long, Record> pkMap, LogIndex logIndex, int index) throws InterruptedException {
        long oldPk = logIndex.getOldPks()[index];
        pkMap.remove(oldPk);
    }

    private void updateResualt(Map<Long, Record> pkMap, LogIndex logIndex, int index) throws InterruptedException {
        long oldPk = logIndex.getOldPks()[index];
        if (!pkMap.containsKey(oldPk)) {
            return;
        }
        Record record = pkMap.get(oldPk);
        long newPk = logIndex.getNewPk(index);
        short columnSize = logIndex.getColumnSize(index);
        for (int i = 0; i < columnSize; i++) {
            int pos = logIndex.getHashColumnName(index)[i];
            ByteBuffer columnByteBuffer = record.getColumnValue().get(pos);

            int columnPos = logIndex.getColumnNewValues(index)[i];
            int columnLen = logIndex.getColumnValueLens(index)[i];
            if (columnLen > columnByteBuffer.capacity()) {
                //重新申请一个更大长度的byteBuffer
                columnByteBuffer = ByteBuffer.allocate(columnLen);
            }
            byteBuffer.position(columnPos);
            System.arraycopy(byteBuffer.array(), columnPos, columnByteBuffer.array(), 0, columnLen);
            columnByteBuffer.limit(columnLen);
        }

        if (oldPk != newPk) {
            pkMap.remove(oldPk);
            pkMap.put(newPk, record);
        }
    }

    private void insertResualt(Map<Long, Record> pkMap, LogIndex logIndex, int index, int redoNum) throws InterruptedException {
        long newPk = logIndex.getNewPk(index);
        if (newPk <= beginPk || newPk >= endPk)
            return;
        //      int logSize = logIndex.getLogSize();
        //每个pk对应到不同的线程
        if(newPk % REDO_NUM == (redoNum - 1)){
        short columnSize = logIndex.getColumnSize(index);
        Record record = new Record(newPk, columnSize);
        for (int i = 0; i < columnSize; i++) {
//            int hashColumnName = logIndex.getHashColumnName()[index][i];
            int columnPos = logIndex.getColumnNewValues(index)[i];
            int columnLen = logIndex.getColumnValueLens(index)[i];

//            record.getColumnName().add(hashColumnName);
            ByteBuffer columnByteBuffer = ByteBuffer.allocate(columnLen);
            byteBuffer.position(columnPos);
            System.arraycopy(byteBuffer.array(), columnPos, columnByteBuffer.array(), 0, columnLen);
            columnByteBuffer.limit(columnLen);
            record.getColumnValue().add(columnByteBuffer);
        }
        pkMap.put(newPk, record);
    }
    }
}
