package org.pangolin.yx.zhengxu;

import org.pangolin.xuzhe.Log;
import org.pangolin.yx.Config;
import org.pangolin.yx.PlainHashArr;
import org.pangolin.yx.PlainHashing;

import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/23.
 */
public class DataStorageTwoLevel implements DataStorage {

    class RecordData {
        long preid;
        byte[] colData;
    }

    static class Level1 {
        byte[] colData;
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
    private static final int OFF_SEQ = 4;
    private static final int OFF_PREID = 12;
    private static final int OFF_FLAG = 20;  //valid=1 表示无效
    private static final int OFF_CELL = 21;
    private static final int FIRST_LEVEL_COUNT = 1 << 20;
    private static final int FIRST_LEVEL_MAX = FIRST_LEVEL_COUNT - 1;
    TableInfo tableInfo;
    int CELL_SIZE;
    int CELL_COUNT;
    int COL_BLOCK_SIZE;
    private static final int BUFFER_SIZE = 1024 * 1024;
    private static final int BUFFER_BITS = 20;
    private int blockSize = 0;
    PlainHashArr hashing = new PlainHashArr(19);
    private ArrayList<byte[]> bytes = new ArrayList<>();
    private int nextBytePos;
    private Level1 level1;

    DataStorageTwoLevel(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        CELL_SIZE = Config.MAX_COL_SIZE + 1;
        CELL_COUNT = tableInfo.columnName.length - 1;
        blockSize = 4 + 8 + 8 + 1 + CELL_SIZE * CELL_COUNT;
        nextBytePos = blockSize;
        bytes.add(new byte[BUFFER_SIZE]);
        COL_BLOCK_SIZE = CELL_SIZE * CELL_COUNT;

        level1 = new Level1();
        level1.colData = new byte[CELL_SIZE * CELL_COUNT * FIRST_LEVEL_COUNT];
        level1.next = new int[FIRST_LEVEL_COUNT];
        level1.preid = new long[FIRST_LEVEL_COUNT];
        level1.seq = new long[FIRST_LEVEL_COUNT];
        level1.flag = new byte[FIRST_LEVEL_COUNT];
    }

    int allocateBlock() {
        int index = nextBytePos / BUFFER_SIZE;
        int off = nextBytePos % BUFFER_SIZE;
        if (off + blockSize > BUFFER_SIZE) {
            bytes.add(new byte[BUFFER_SIZE]);
            nextBytePos = (bytes.size() - 1) * BUFFER_SIZE;
        }
        int re = nextBytePos;
        nextBytePos += blockSize;

        byte[] data = bytes.get(re / BUFFER_SIZE);
        off = re % BUFFER_SIZE;
        //set length to -1
        for (int i = 0; i < CELL_COUNT; i++) {
            data[off + OFF_CELL + CELL_SIZE * i] = -1;
        }

        return re;
    }

    public long getSeq(int node) {
        return readLong(bytes, node + OFF_SEQ);
    }

    public byte getValid(int node) {
        return readByte(bytes, node + OFF_FLAG);
    }

    public long getPreid(int node) {
        return readLong(bytes, node + OFF_PREID);
    }


    private void writeDataToBytesRaw(byte[] buff, int bufPos, LogRecord logRecord, byte[] readBuff) {
        int[] logData = logRecord.columnData;
        int datalen = logRecord.columnData.length;
        for (int i = 0; i < datalen; ) {
            int index = logData[i++];
            if (index == 0) {
                break;
            } else {
                int pos = logData[i++];
                int len = logData[i++];
                int writePos = (index - 1) * CELL_SIZE + bufPos;
                //bytes[writePos] = (byte) len;
                //writeByte(bytes, node + OFF_CELL + writePos, (byte) len);
                buff[writePos] = (byte) len;
                //System.arraycopy(logRecord.lineData, pos, bytes, writePos + 1, len);
                //writeBytes(bytes, node + OFF_CELL + writePos + 1, readBuff, pos, len);
                for (int j = 0; j < len; j++) {
                    buff[writePos + 1 + j] = readBuff[pos + j];
                }
            }
        }
    }

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

    private long readLong(ArrayList<byte[]> buffer, int off) {
        //int index = off / BUFFER_SIZE;
        //int buffOff = off % BUFFER_SIZE;
        int index = off >>> BUFFER_BITS;
        int buffOff = off & (BUFFER_SIZE - 1);
        byte[] buf = buffer.get(index);
        long re = (((long) buf[buffOff]) & 0xff)
                | (((long) buf[buffOff + 1] & 0xff) << 8)
                | (((long) buf[buffOff + 2] & 0xff) << 16)
                | (((long) buf[buffOff + 3] & 0xff) << 24)
                | (((long) buf[buffOff + 4] & 0xff) << 32)
                | (((long) buf[buffOff + 5] & 0xff) << 40)
                | (((long) buf[buffOff + 6] & 0xff) << 48)
                | (((long) buf[buffOff + 7] & 0xff) << 56);
        return re;
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


    public RecordData getRecord(long id, long seq) throws Exception {
        RecordData logRecord = new RecordData();
        int level1Index = (int) (id / Config.REBUILDER_THREAD);
        if (seq == -1) {
            if (level1.flag[level1Index] == FLAG_VALID) {
                logRecord.preid = level1.preid[level1Index];
                logRecord.colData = new byte[COL_BLOCK_SIZE];
                System.arraycopy(level1.colData, COL_BLOCK_SIZE * level1Index, logRecord.colData, 0, COL_BLOCK_SIZE);
                return logRecord;
            }
        } else {
            if (level1.flag[level1Index] == FLAG_VALID) {
                if (level1.seq[level1Index] < seq) {
                    logRecord.preid = level1.preid[level1Index];
                    logRecord.colData = new byte[COL_BLOCK_SIZE];
                    System.arraycopy(level1.colData, COL_BLOCK_SIZE * level1Index, logRecord.colData, 0, COL_BLOCK_SIZE);
                    return logRecord;
                } else {
                    int next = level1.next[level1Index];
                    while (true) {
                        long seqNext = readLong(bytes, next + OFF_SEQ);
                        if (seqNext < seq) {
                            logRecord.preid = readLong(bytes, next + OFF_PREID);
                            logRecord.colData = new byte[COL_BLOCK_SIZE];
                            readBytes(next, 0, logRecord.colData, 0, COL_BLOCK_SIZE);
                            return logRecord;
                        } else {
                            next = readInt(bytes, next + OFF_NEXT);
                        }
                    }
                }
            } else {
                throw new Exception("error");
            }
        }
        throw new Exception("error");
    }

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

    int copyDataToLevel2(int index) {
        int next = level1.next[index];
        int node = allocateBlock();
        writeInt(bytes, node + OFF_NEXT, next);
        writeLong(bytes, node + OFF_SEQ, level1.seq[index]);
        writeLong(bytes, node + OFF_PREID, level1.preid[index]);
        writeByte(bytes, node + OFF_FLAG, level1.flag[index]);
        //writeInt(bytes,node+OFF_NEXT,next);
        writeBytes(bytes, node + OFF_CELL, level1.colData, index * COL_BLOCK_SIZE, COL_BLOCK_SIZE);

        return 0;
    }


    @Override
    public void doLog(LogRecord logRecord, byte[] data) throws Exception {
        if (logRecord.opType == 'U') {
            long id = logRecord.id;
            if (logRecord.preId != logRecord.id) {
                int level1Index = (int) (id / Config.REBUILDER_THREAD);
                if (level1Index > FIRST_LEVEL_MAX) {
                    int next = hashing.getOrDefault(id, 0);
                    int newNode = allocateBlock();
                    writeInt(bytes, OFF_NEXT + newNode, next);
                    writeLong(bytes, OFF_SEQ + newNode, logRecord.seq);
                    writeLong(bytes, OFF_PREID + newNode, logRecord.preId);
                    writeDataToBytes(newNode, logRecord, data);
                    hashing.put(id, newNode);
                } else {
                    if (level1.flag[level1Index] != FLAG_EMPTY) {
                        level1.next[level1Index] = copyDataToLevel2(level1Index);
                    }
                    level1.seq[level1Index] = logRecord.seq;
                    level1.preid[level1Index] = logRecord.preId;
                    level1.flag[level1Index] = FLAG_VALID;
                    writeDataToBytesRaw(level1.colData, level1Index * COL_BLOCK_SIZE, logRecord, data);
                }
                /*
                int next = hashing.getOrDefault(id, 0);
                int newNode = allocateBlock();
                writeInt(bytes, OFF_NEXT + newNode, next);
                writeLong(bytes, OFF_SEQ + newNode, logRecord.seq);
                writeLong(bytes, OFF_PREID + newNode, logRecord.preId);
                writeDataToBytes(newNode, logRecord, data);
                hashing.put(id, newNode);
                */
            } else {
                //更新数据
                int level1Index = (int) (id / Config.REBUILDER_THREAD);
                if (level1Index > FIRST_LEVEL_MAX) {
                    int node = hashing.getOrDefault(id, 0);
                    writeDataToBytes(node, logRecord, data);
                } else {
                    writeDataToBytesRaw(level1.colData, level1Index * COL_BLOCK_SIZE, logRecord, data);
                }

                /*
                int node = hashing.getOrDefault(id, 0);
                if (node == 0) {
                    System.out.println(id);
                    throw new Exception("no preid in data");
                } else {
                    writeDataToBytes(node, logRecord, data);
                }
                */

            }
        } else if (logRecord.opType == 'I') {
            long id = logRecord.id;
            int level1Index = (int) (id / Config.REBUILDER_THREAD);
            if (level1Index > FIRST_LEVEL_MAX) {
                int next = hashing.getOrDefault(id, 0);
                int newNode = allocateBlock();
                writeInt(bytes, OFF_NEXT + newNode, next);
                writeLong(bytes, OFF_SEQ + newNode, logRecord.seq);
                writeLong(bytes, OFF_PREID + newNode, -1);
                writeDataToBytes(newNode, logRecord, data);
                hashing.put(id, newNode);
            } else {
                if (level1.flag[level1Index] != FLAG_EMPTY) {
                    level1.next[level1Index] = copyDataToLevel2(level1Index);
                }
                level1.seq[level1Index] = logRecord.seq;
                level1.preid[level1Index] = -1;
                level1.flag[level1Index] = FLAG_VALID;
                writeDataToBytesRaw(level1.colData, level1Index * COL_BLOCK_SIZE, logRecord, data);
            }

            /*
            int next = hashing.getOrDefault(id, 0);
            int newNode = allocateBlock();
            writeInt(bytes, OFF_NEXT + newNode, next);
            writeLong(bytes, OFF_SEQ + newNode, logRecord.seq);
            writeLong(bytes, OFF_PREID + newNode, -1);
            writeDataToBytes(newNode, logRecord, data);
            hashing.put(id, newNode);
            */
        } else if (logRecord.opType == 'D') {
            long id = logRecord.preId;
            int level1Index = (int) (id / Config.REBUILDER_THREAD);
            if (level1Index > FIRST_LEVEL_MAX) {
                int node = hashing.getOrDefault(id, 0);
                if (node == 0) {
                    throw new Exception("error");
                } else {
                    int next = readInt(bytes, node + OFF_NEXT);
                    if (next == 0) {
                        hashing.remove(id);
                    } else {
                        hashing.put(id, next);
                    }
                }
            } else {
                if (level1.next[level1Index] != 0) {
                    //read the pre node
                    int next = level1.next[level1Index];
                    level1.next[level1Index] = readInt(bytes, next + OFF_NEXT);
                    level1.seq[level1Index] = readLong(bytes, next + OFF_SEQ);
                    level1.flag[level1Index] = readByte(bytes, next + OFF_FLAG);
                    level1.preid[level1Index] = readLong(bytes, next + OFF_PREID);
                    //level1.colData[level1Index] = readInt(bytes, next + OFF_NEXT);
                    readBytes(next, OFF_CELL, level1.colData, level1Index * COL_BLOCK_SIZE, COL_BLOCK_SIZE);
                } else {
                    level1.flag[level1Index] = FLAG_EMPTY;
                }
            }

            /*
            long id = logRecord.preId;
            int node = hashing.getOrDefault(id, 0);
            if (node == 0) {
                throw new Exception("error");
            } else {
                int next = readInt(bytes, node + OFF_NEXT);
                if (next == 0) {
                    hashing.remove(id);
                } else {
                    hashing.put(id, next);
                }
            }
            */

        } else if (logRecord.opType == 'X') {
            long id = logRecord.id;
            int level1Index = (int) (id / Config.REBUILDER_THREAD);
            if (level1Index > FIRST_LEVEL_MAX) {
                int node = hashing.get(id);
                writeByte(bytes, OFF_FLAG + node, (byte) 1);
            } else {
                level1.flag[level1Index] = FLAG_X;
            }
            /*
            long id = logRecord.id;
            int node = hashing.get(id);
            writeByte(bytes, OFF_VALID + node, (byte) 1);
            */
        }
    }

}
