package org.pangolin.yx.zhengxu;

import org.pangolin.yx.Config;
import org.pangolin.yx.PlainHashing;

import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/22.
 */
public class DataSotragePlainSimple implements DataStorage {
    //存储格式:  next/int seq/long preid/long valid/byte [len/byte data/6byte]*n
    private static final int OFF_NEXT = 0;
    private static final int OFF_SEQ = 4;
    private static final int OFF_PREID = 12;
    private static final int OFF_VALID = 20;  //valid=1 表示无效
    private static final int OFF_CELL = 21;
    TableInfo tableInfo;
    int CELL_SIZE;
    int CELL_COUNT;
    private int blockSize = 0;
    //LinearHashing hashing = new LinearHashing();
    PlainHashing hashing = new PlainHashing(20);
    //private ArrayList<byte[]> bytes = new ArrayList<>();
    private byte[] buff = new byte[1024 * 1024 * 80];
    private int nextBytePos;

    DataSotragePlainSimple(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        CELL_SIZE = Config.MAX_COL_SIZE + 1;
        CELL_COUNT = tableInfo.columnName.length - 1;
        blockSize = 4 + 8 + 8 + 1 + CELL_SIZE * CELL_COUNT;
        nextBytePos = blockSize;
        // bytes.add(new byte[BUFFER_SIZE]);
    }

    int allocateBlock() throws Exception {
        int re = nextBytePos;
        nextBytePos += blockSize;
        if (nextBytePos > buff.length) {
            throw new Exception("buff full");
        }
        return re;
    }

    public long getSeq(int node) {
        return readLong(node + OFF_SEQ);
    }

    public byte getValid(int node) {
        return readByte(node + OFF_VALID);
    }

    public long getPreid(int node) {
        return readLong(node + OFF_PREID);
    }


    private void writeDataToBytes(int node, LogRecord logRecord, byte[] readBuff) {
        int[] logData = logRecord.columnData;
        int size = logRecord.columnData.length / 3;
        for (int i = 0; i < size; i++) {
            int index = logData[i * 3];
            if (index == 0) {
                break;
            } else {
                int pos = logData[i * 3 + 1];
                int len = logData[i * 3 + 2];
                int writePos = (index - 1) * CELL_SIZE;
                //bytes[writePos] = (byte) len;
                writeByte(node + OFF_CELL + writePos, (byte) len);

                //System.arraycopy(logRecord.lineData, pos, bytes, writePos + 1, len);
                writeBytes(node + OFF_CELL + writePos + 1, readBuff, pos, len);
            }
        }
    }

    private int readInt(int off) {
        int re = (buff[off] & 0xff)
                | ((buff[off + 1] & 0xff) << 8)
                | ((buff[off + 2] & 0xff) << 16)
                | ((buff[off + 3] & 0xff) << 24);
        return re;
    }

    private byte readByte(int off) {
        byte re = buff[off];
        return re;
    }

    public void readBytes(int node, int srcPos, byte[] dst, int dstPos, int len) {
        System.arraycopy(buff, node + OFF_CELL + srcPos, dst, dstPos, len);
    }

    private long readLong(int off) {
        long re = (((long) buff[off]) & 0xff)
                | (((long) buff[off + 1] & 0xff) << 8)
                | (((long) buff[off + 2] & 0xff) << 16)
                | (((long) buff[off + 3] & 0xff) << 24)
                | (((long) buff[off + 4] & 0xff) << 32)
                | (((long) buff[off + 5] & 0xff) << 40)
                | (((long) buff[off + 6] & 0xff) << 48)
                | (((long) buff[off + 7] & 0xff) << 56);
        return re;
    }

    private void writeBytes(int off, byte[] src, int srcPos, int srcLen) {
        //System.arraycopy(src,srcPos,buf,buffOff,srcLen);
        for (int i = 0; i < srcLen; i++) {
            buff[off + i] = src[srcPos + i];
        }
    }

    private void writeInt(int off, int v) {
        buff[off] = (byte) (0xff & v);
        buff[off + 1] = (byte) (0xff & v >>> 8);
        buff[off + 2] = (byte) (0xff & v >>> 16);
        buff[off + 3] = (byte) (0xff & v >>> 24);
    }

    private void writeByte(int off, byte v) {
        buff[off] = v;
    }

    private void writeLong(int off, long v) {
        buff[off] = (byte) (0xff & v);
        buff[off + 1] = (byte) (0xff & v >>> 8);
        buff[off + 2] = (byte) (0xff & v >>> 16);
        buff[off + 3] = (byte) (0xff & v >>> 24);

        buff[off + 4] = (byte) (0xff & v >>> 32);
        buff[off + 5] = (byte) (0xff & v >>> 40);
        buff[off + 6] = (byte) (0xff & v >>> 48);
        buff[off + 7] = (byte) (0xff & v >>> 56);
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
                    long nodeSeq = readLong(node + OFF_SEQ);
                    if (nodeSeq >= seq) {
                        int next = readInt(node + OFF_NEXT);
                        node = next;
                    } else {
                        break;
                    }
                }
                return node;
            }
        }
    }

    @Override
    public void doLog(LogRecord logRecord, byte[] data) throws Exception {
        if (logRecord.opType == 'U') {
            long id = logRecord.id;
            if (logRecord.preId != logRecord.id) {
                int next = hashing.getOrDefault(id, 0);
                int newNode = allocateBlock();
                writeInt(OFF_NEXT + newNode, next);
                writeLong(OFF_SEQ + newNode, logRecord.seq);
                writeLong(OFF_PREID + newNode, logRecord.preId);
                writeDataToBytes(newNode, logRecord, data);
                hashing.put(id, newNode);
            } else {
                int node = hashing.getOrDefault(id, 0);
                if (node == 0) {
                    System.out.println(id);
                    throw new Exception("no preid in data");
                } else {
                    writeDataToBytes(node, logRecord, data);
                }

            }
        } else if (logRecord.opType == 'I') {
            long id = logRecord.id;
            int next = hashing.getOrDefault(id, 0);
            int newNode = allocateBlock();
            writeInt(OFF_NEXT + newNode, next);
            writeLong(OFF_SEQ + newNode, logRecord.seq);
            writeLong(OFF_PREID + newNode, -1);
            writeDataToBytes(newNode, logRecord, data);
            hashing.put(id, newNode);
        } else if (logRecord.opType == 'D') {
            long id = logRecord.preId;
            int node = hashing.getOrDefault(id, 0);
            if (node == 0) {
                throw new Exception("error");
            } else {
                int next = readInt(node + OFF_NEXT);
                if (next == 0) {
                    hashing.remove(id);
                } else {
                    hashing.put(id, next);
                }
            }

        } else if (logRecord.opType == 'X') {
            long id = logRecord.id;
            int node = hashing.get(id);
            writeByte(OFF_VALID + node, (byte) 1);
        }
    }
}
