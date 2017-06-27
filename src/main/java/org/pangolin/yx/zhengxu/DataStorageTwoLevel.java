package org.pangolin.yx.zhengxu;

import org.pangolin.xuzhe.Log;
import org.pangolin.xuzhe.positiveorder.MyLong2IntHashMap;
import org.pangolin.yx.Config;
import org.pangolin.yx.PlainHashArr;
import org.pangolin.yx.PlainHashing;
import org.pangolin.yx.PlainHashingSimple;

import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/23.
 */
class RecordData {
    long preid;
    long seq;
    long[] colData;
}

public class DataStorageTwoLevel implements DataStorage {


    static class Level1 {
        long[] colData;
        int[] next;
        long[] seq;
        byte[] flag;//0 empty 1 valid 2 non-valid
        long[] preid;
    }

    public static final int FLAG_EMPTY = 0;
    public static final int FLAG_VALID = 1;
    public static final int FLAG_X = 2;
    //存储格式:  next/int seq/long preid/long valid/byte [len/byte data/6byte]*n
    private static final int OFF_NEXT = 0;
    private static final int OFF_SEQ = 1;
    private static final int OFF_PREID = 2;
    private static final int OFF_FLAG = 3;  //valid=1 表示无效
    private static final int OFF_CELL = 4;
    private static final int FIRST_LEVEL_COUNT = 1 << 21;
    private static final int FIRST_LEVEL_MAX = FIRST_LEVEL_COUNT - 1;
    TableInfo tableInfo;
    public static int CELL_SIZE;
    public static int CELL_COUNT;
    public static int COL_BLOCK_SIZE;
    private static final int BUFFER_SIZE = 1024 * 1024;
    private static final int BUFFER_BITS = 20;
    private static int blockSize = 0;
    private final PlainHashing hashing = new PlainHashing(20);
    //HashWrapper  hashing = new HashWrapper ();
    private final ArrayList<long[]> bytes = new ArrayList<>();
    private int nextBytePos;
    private static Level1 level1;


    public static void init(TableInfo tableInfo) {
        CELL_SIZE = 1;//1字节长度6字节数据1字节位置
        CELL_COUNT = GlobalData.colCount;
        blockSize = 1 + 1 + 1 + 1 + CELL_SIZE * CELL_COUNT;
        COL_BLOCK_SIZE = CELL_SIZE * CELL_COUNT;
        level1 = new Level1();
        level1.colData = new long[CELL_SIZE * CELL_COUNT * Config.ALI_ID_MAX];
        level1.next = new int[Config.ALI_ID_MAX];
        level1.preid = new long[Config.ALI_ID_MAX];
        level1.seq = new long[Config.ALI_ID_MAX];
        level1.flag = new byte[Config.ALI_ID_MAX];
    }

    DataStorageTwoLevel(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        nextBytePos = blockSize;
        bytes.add(new long[BUFFER_SIZE]);
    }

    int allocateBlock() {
        int off = nextBytePos % BUFFER_SIZE;
        if (off + blockSize >= BUFFER_SIZE) {
            bytes.add(new long[BUFFER_SIZE]);
            nextBytePos = (bytes.size() - 1) * BUFFER_SIZE;
        }
        int re = nextBytePos;
        nextBytePos += blockSize;

        return re;
    }

    public long getSeq(int node) {
        return readLong(node + OFF_SEQ);
    }

    public byte getValid(int node) {
        return (byte) readLong(node + OFF_FLAG);
    }

    public long getPreid(int node) {
        return readLong(node + OFF_PREID);
    }


    private void writeLogDataToLevel1(int bufPos, long[] logData, int logDataPos, int len) {
        long[] target = level1.colData;
        for (int i = 0; i < len; i++) {
            long colv = logData[logDataPos + i];
            int pos = (int) (logData[logDataPos + i] >> 56);
            target[(bufPos + pos - 1)] = colv;
        }
    }

    private void writeLogDataToLevel2(int off, long[] logData, int logDataPos, int len) {
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        long[] target = bytes.get(index);
        for (int i = 0; i < len; i++) {
            long colv = logData[logDataPos + i];
            int pos = (int) (logData[logDataPos + i] >> 56);
            target[(buffOff + pos - 1)] = colv;
        }
    }
/*
    private void writeDataToBytes(int node, LogRecord logRecord, byte[] readBuff) {
        int[] logData = logRecord.columnData;
        int datalen = logRecord.columnData.length;
        for (int i = 0; i < datalen; ) {
            int index = logData[i++];
            if (index == 0) {
                break;
            } else {
                int pos = logData[i++];
                int len = logData[i++];
                int writePos = (index - 1) * CELL_SIZE;
                //bytes[writePos] = (byte) len;
                writeByte(bytes, node + OFF_CELL + writePos, (byte) len);

                //System.arraycopy(logRecord.lineData, pos, bytes, writePos + 1, len);
                writeBytes(bytes, node + OFF_CELL + writePos + 1, readBuff, pos, len);
            }
        }
    }

    private void writeDataToBytesDirect(int node, int[] logData, int logDataOff, byte[] readBuff) {
        for (int i = 0; i < 3 * GlobalData.colCount; ) {
            int index = logData[logDataOff + i++];
            if (index == 0) {
                break;
            } else {
                int pos = logData[logDataOff + i++];
                int len = logData[logDataOff + i++];
                int writePos = (index - 1) * CELL_SIZE;
                //bytes[writePos] = (byte) len;
                writeByte(bytes, node + OFF_CELL + writePos, (byte) len);

                //System.arraycopy(logRecord.lineData, pos, bytes, writePos + 1, len);
                writeBytes(bytes, node + OFF_CELL + writePos + 1, readBuff, pos, len);
            }
        }
    }
    */
/*
    private int readInt(ArrayList<byte[]> buffer, int off) {
        //int index = off / BUFFER_SIZE;
        //int buffOff = off % BUFFER_SIZE;
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        byte[] buf = buffer.get(index);
        int re = (buf[buffOff] & 0xff)
                | ((buf[buffOff + 1] & 0xff) << 8)
                | ((buf[buffOff + 2] & 0xff) << 16)
                | ((buf[buffOff + 3] & 0xff) << 24);
        return re;
    }
    */
/*
    private byte readByte(ArrayList<byte[]> buffer, int off) {
        //int index = off / BUFFER_SIZE;
        //int buffOff = off % BUFFER_SIZE;
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        byte[] buf = buffer.get(index);
        byte re = buf[buffOff];
        return re;
    }

    public void readBytes(int node, int srcPos, byte[] dst, int dstPos, int len) {
//        int index = node / BUFFER_SIZE;
        //      int buffOff = node % BUFFER_SIZE;
        int index = node >>> BUFFER_BITS;
        int buffOff = node & (BUFFER_SIZE - 1);
        byte[] buf = bytes.get(index);
        System.arraycopy(buf, buffOff + OFF_CELL + srcPos, dst, dstPos, len);
    }
    */

    private long readLong(int off) {
        //int index = off / BUFFER_SIZE;
        //int buffOff = off % BUFFER_SIZE;
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        long[] buf = bytes.get(index);
        return buf[buffOff];
    }

    private void writeBytes(ArrayList<byte[]> buffer, int off, byte[] src, int srcPos, int srcLen) {
        //int index = off / BUFFER_SIZE;
        //int buffOff = off % BUFFER_SIZE;
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        byte[] buf = buffer.get(index);
        //System.arraycopy(src,srcPos,buf,buffOff,srcLen);
        for (int i = 0; i < srcLen; i++) {
            buf[buffOff + i] = src[srcPos + i];
        }
    }


    private void writeByte(ArrayList<byte[]> buffer, int off, byte v) {
        //int index = off / BUFFER_SIZE;
        //int buffOff = off % BUFFER_SIZE;
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        byte[] buf = buffer.get(index);
        buf[buffOff] = v;
    }

    private void writeInt(ArrayList<byte[]> buffer, int off, int v) {
        //int index = off / BUFFER_SIZE;
        //int buffOff = off % BUFFER_SIZE;
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        byte[] buf = buffer.get(index);
        buf[buffOff] = (byte) (0xff & v);
        buf[buffOff + 1] = (byte) (0xff & v >>> 8);
        buf[buffOff + 2] = (byte) (0xff & v >>> 16);
        buf[buffOff + 3] = (byte) (0xff & v >>> 24);
    }

    private void writeLongToLevel2(int off, long v) {
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        long[] buf = bytes.get(index);
        buf[buffOff] = v;
    }

    private void writeLong(ArrayList<byte[]> buffer, int off, long v) {
        //int index = off / BUFFER_SIZE;
        //int buffOff = off % BUFFER_SIZE;
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        byte[] buf = buffer.get(index);
        buf[buffOff] = (byte) (0xff & v);
        buf[buffOff + 1] = (byte) (0xff & v >>> 8);
        buf[buffOff + 2] = (byte) (0xff & v >>> 16);
        buf[buffOff + 3] = (byte) (0xff & v >>> 24);

        buf[buffOff + 4] = (byte) (0xff & v >>> 32);
        buf[buffOff + 5] = (byte) (0xff & v >>> 40);
        buf[buffOff + 6] = (byte) (0xff & v >>> 48);
        buf[buffOff + 7] = (byte) (0xff & v >>> 56);
    }


    private RecordData getRecordLevel1(long id, long seq) throws Exception {

        //int level1Index = (int) (id / Config.REBUILDER_THREAD);
        int level1Index = (int) (id);
        if (seq == -1) {
            if (level1.flag[level1Index] == FLAG_VALID) {
                RecordData logRecord = new RecordData();
                logRecord.preid = level1.preid[level1Index];
                logRecord.colData = new long[COL_BLOCK_SIZE];
                logRecord.seq = level1.seq[level1Index];
                System.arraycopy(level1.colData, COL_BLOCK_SIZE * level1Index, logRecord.colData, 0, COL_BLOCK_SIZE);
                return logRecord;
            }
        } else {
            if (level1.seq[level1Index] < seq) {
                RecordData logRecord = new RecordData();
                logRecord.preid = level1.preid[level1Index];
                logRecord.colData = new long[COL_BLOCK_SIZE];
                logRecord.seq = level1.seq[level1Index];
                System.arraycopy(level1.colData, COL_BLOCK_SIZE * level1Index, logRecord.colData, 0, COL_BLOCK_SIZE);
                return logRecord;
            } else {
                int next = level1.next[level1Index];
                while (true) {
                    long seqNext = readLong(next + OFF_SEQ);
                    if (seqNext < seq) {
                        RecordData logRecord = new RecordData();
                        logRecord.preid = readLong(next + OFF_PREID);
                        logRecord.colData = new long[COL_BLOCK_SIZE];
                        logRecord.seq = readLong(next + OFF_SEQ);
                        //readBytes(next, 0, logRecord.colData, 0, COL_BLOCK_SIZE);
                        readLongs(next + OFF_CELL, logRecord.colData, 0, COL_BLOCK_SIZE);
                        return logRecord;
                    } else {
                        next = (int) readLong(next + OFF_NEXT);
                    }
                }
            }

        }
        return null;
    }

    private RecordData getRecordLevel2(long id, long seq) throws Exception {
        RecordData logRecord = new RecordData();
        int node = hashing.getOrDefault(id, 0);
        if (node == 0) {
            return null;
        } else {
            if (seq != -1) {
                while (node != 0) {
                    long nodeSeq = readLong(node + OFF_SEQ);
                    if (nodeSeq >= seq) {
                        int next = (int) readLong(node + OFF_NEXT);
                        node = next;
                    } else {
                        break;
                    }
                }
            } else {
                //check valid
                byte valid = getValid(node);
                if (valid == 1) {
                    return null;
                }
            }
            logRecord.seq = getSeq(node);
            logRecord.preid = getPreid(node);
            logRecord.colData = new long[COL_BLOCK_SIZE];
            //readBytes(node, 0, logRecord.colData, 0, logRecord.colData.length);
            readLongs(node + OFF_CELL, logRecord.colData, 0, logRecord.colData.length);
            return logRecord;
        }
    }

    public RecordData getRecord(long id, long seq) throws Exception {
        return getRecordLevel1(id, seq);
        /*
        if (id / Config.REBUILDER_THREAD > FIRST_LEVEL_MAX) {
            return getRecordLevel2(id, seq);
        } else {
            return getRecordLevel1(id, seq);
        }
        */
    }
/*
    //seq=-1 表示不关心seq
    public int getNode(long id, long seq) throws Exception {
        int node = hashing.getOrDefault(id, 0);
        if (node == 0) {
            return 0;
        } else {
            if (seq == -1) {
                return node;
            } else {
                while (node != 0) {
                    long nodeSeq = readLong(bytes, node + OFF_SEQ);
                    if (nodeSeq >= seq) {
                        int next = readInt(bytes, node + OFF_NEXT);
                        node = next;
                    } else {
                        break;
                    }
                }
                return node;
            }
        }
    }
    */


    void clearColData(int index) {
        //clear old data
        for (int i = 0; i < COL_BLOCK_SIZE; i++) {
            level1.colData[index * COL_BLOCK_SIZE + i] = 0;
        }
        //
    }

    private void writeLongs(int off, long[] longs, int pos, int len) {
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        long[] buf = bytes.get(index);
        for (int i = 0; i < len; i++) {
            buf[buffOff + i] = longs[pos + i];
        }
    }

    private void readLongs(int off, long[] longs, int pos, int len) {
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        long[] buf = bytes.get(index);
        for (int i = 0; i < len; i++) {
            //buf[buffOff + i] = longs[pos + i];
            longs[pos + i] = buf[buffOff + i];
        }
    }

    int copyDataToLevel2(int index) {
        int next = level1.next[index];
        int node = allocateBlock();
        writeLongToLevel2(node + OFF_NEXT, next);
        writeLongToLevel2(node + OFF_SEQ, level1.seq[index]);
        writeLongToLevel2(node + OFF_PREID, level1.preid[index]);
        writeLongToLevel2(node + OFF_FLAG, level1.flag[index]);
        //writeInt(bytes,node+OFF_NEXT,next);
        //writeBytes(bytes, node + OFF_CELL, level1.colData, index * COL_BLOCK_SIZE, COL_BLOCK_SIZE);
        writeLongs(node + OFF_CELL, level1.colData, index * COL_BLOCK_SIZE, COL_BLOCK_SIZE);
        clearColData(index);
        return node;
    }


    @Override
    public void doLog(LogRecord logRecord, byte[] data) throws Exception {

    }

    @Override
    public void doLog(LogBlock logBlock, byte[] data, int logPos) throws Exception {

        long id = logBlock.ids[logPos];
        long preId = logBlock.preIds[logPos];

        if (id >= Config.ALI_ID_MAX || preId >= Config.ALI_ID_MAX) {
            return;
        }

        byte opType = logBlock.opTypes[logPos];
        //long seq = logBlock.seqs[logPos];
        long[] colData = logBlock.colData;
        //int colDataPos = logPos * GlobalData.colCount;
        //提取列数据信息
        int colDataPos = logBlock.colDataInfo[logPos];
        int colDataLen = colDataPos & 0xff;
        colDataPos = colDataPos >> 8;
        /*
        long offLocal;
        if (opType == 'D') {
            offLocal = preId / Config.REBUILDER_THREAD;
        } else {
            offLocal = id / Config.REBUILDER_THREAD;
        }
        */
        int level1Index = (int) id;
        if (opType == 'D') {
            level1Index = (int) preId;
        }
        if (opType == 'U') {
            if (preId != id) {
                if (level1.flag[level1Index] != FLAG_EMPTY) {
                    level1.next[level1Index] = copyDataToLevel2(level1Index);
                }
                level1.seq[level1Index] = logBlock.seqs[logPos];
                level1.preid[level1Index] = preId;
                level1.flag[level1Index] = FLAG_VALID;
                //writeDataToBytesRaw(level1.colData, level1Index * COL_BLOCK_SIZE, colData, colDataPos, data);
                writeLogDataToLevel1(level1Index * COL_BLOCK_SIZE, colData, colDataPos, colDataLen);
            } else {
                //writeDataToBytesRaw(level1.colData, level1Index * COL_BLOCK_SIZE, colData, colDataPos, data);
                writeLogDataToLevel1(level1Index * COL_BLOCK_SIZE, colData, colDataPos, colDataLen);
            }
        } else if (opType == 'I') {
            if (level1.flag[level1Index] != FLAG_EMPTY) {
                level1.next[level1Index] = copyDataToLevel2(level1Index);
            }
            level1.seq[level1Index] = logBlock.seqs[logPos];
            level1.preid[level1Index] = -1;
            level1.flag[level1Index] = FLAG_VALID;
            //writeDataToBytesRaw(level1.colData, level1Index * COL_BLOCK_SIZE, colData, colDataPos, data);
            writeLogDataToLevel1(level1Index * COL_BLOCK_SIZE, colData, colDataPos, colDataLen);
        } else if (opType == 'D') {
            if (level1.next[level1Index] != 0) {
                //read the pre node
                int next = level1.next[level1Index];
                level1.next[level1Index] = (int) readLong(next + OFF_NEXT);
                level1.seq[level1Index] = readLong(next + OFF_SEQ);
                level1.flag[level1Index] = (byte) readLong(next + OFF_FLAG);
                level1.preid[level1Index] = readLong(next + OFF_PREID);
                //level1.colData[level1Index] = readInt(bytes, next + OFF_NEXT);
                //readBytes(next, OFF_CELL, level1.colData, level1Index * COL_BLOCK_SIZE, COL_BLOCK_SIZE);
                readLongs(next + OFF_CELL, level1.colData, level1Index * COL_BLOCK_SIZE, COL_BLOCK_SIZE);
            } else {
                level1.flag[level1Index] = FLAG_EMPTY;
                clearColData(level1Index);
            }
        } else if (opType == 'X') {
            level1.flag[level1Index] = FLAG_X;
        }
    }

}
