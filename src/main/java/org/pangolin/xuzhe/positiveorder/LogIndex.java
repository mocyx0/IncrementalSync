package org.pangolin.xuzhe.positiveorder;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.pangolin.xuzhe.positiveorder.Constants.LOGINDEX_SIZE;
import static org.pangolin.xuzhe.positiveorder.Constants.PARSER_NUM;

/**
 * Created by 29146 on 2017/6/16.
 */
public final class LogIndex {
    public static final LogIndex EMPTY_LOG_INDEX = new LogIndex(0, null);
    private long[] oldPk;
    private long[] newPk;
    private byte[] logType;
    private int[][] hashColumnName;   //列名的hash值
    private short[][] columnLen;
    private int[][] columnNewValues;          //列值
    private short[] columnSize;
    private int logSize;
    private ByteBuffer byteBuffer;
    private LogIndexPool pool;
    private AtomicInteger refCount = new AtomicInteger(PARSER_NUM);
    /**
     *
     * @param columnCount  根据insert确定的表中列数
     */
    public LogIndex(int columnCount, LogIndexPool pool) {
        oldPk = new long[LOGINDEX_SIZE];
        newPk = new long[LOGINDEX_SIZE];
        logType = new byte[LOGINDEX_SIZE];
        hashColumnName = new int[LOGINDEX_SIZE][columnCount];
        columnLen = new short[LOGINDEX_SIZE][columnCount];
        columnNewValues = new int[LOGINDEX_SIZE][columnCount];
        columnSize = new short[LOGINDEX_SIZE];
        logSize = 0;
        this.pool = pool;
    }

    public void setLogSize(int logSize) {
        this.logSize = logSize;
    }

    public void addNewLog(long oldPK, long newPK, byte logType, int logItemIndex) {
        this.oldPk[logItemIndex] = oldPK;
        this.newPk[logItemIndex] = newPK;
        this.logType[logItemIndex] = logType;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public void setByteBuffer(ByteBuffer byteBuffer) {

        this.byteBuffer = byteBuffer;
    }

    public int[] getHashColumnName(int logIndex) {
        return hashColumnName[logIndex];
    }

    public int getLogSize() {
        return logSize;
    }

    public long[] getOldPks() {
        return oldPk;
    }

    public long getNewPk(int logIndex) {
        return newPk[logIndex];
    }

    public byte getLogType(int logIndex) {
        return logType[logIndex];
    }

    public short[] getColumnValueLens(int logIndex) {
        return columnLen[logIndex];
    }

    public int[] getColumnNewValues(int logIndex) {
        return columnNewValues[logIndex];
    }

    public short getColumnSize(int logIndex) {
        return columnSize[logIndex];
    }

    public void setColumnSize(int logIndex, int columnSize) {
        this.columnSize[logIndex] = (short)columnSize;
    }


    public void reset() {
        logSize = 0;
    }

    public synchronized void release() {
        if(refCount.decrementAndGet() == 0) {
            try {
                ReadBufferPool.getInstance().put(byteBuffer);
                pool.put(this);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
