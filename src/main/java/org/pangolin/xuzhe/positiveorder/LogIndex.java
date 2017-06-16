package org.pangolin.xuzhe.positiveorder;

import java.nio.ByteBuffer;

/**
 * Created by 29146 on 2017/6/16.
 */
public class LogIndex {
    private long[] oldPk;
    private long[] newPk;
    private byte[] logType;
    private int[] hashColumnName;   //列名的hash值
    private short[][] columnLen;
    private short[][] column;          //列值
    private short[] columnSize;
    private int logSize;
    private ByteBuffer byteBuffer;

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public void setByteBuffer(ByteBuffer byteBuffer) {

        this.byteBuffer = byteBuffer;
    }

    public int[] getHashColumnName() {
        return hashColumnName;
    }

    public int getLogSize() {
        return logSize;
    }

    public long[] getOldPk() {
        return oldPk;
    }

    public long[] getNewPk() {
        return newPk;
    }

    public byte[] getLogType() {
        return logType;
    }

    public short[][] getColumn() {
        return column;
    }

    public short getColumnSize(int logIndex) {
        return columnSize[logIndex];
    }
}
