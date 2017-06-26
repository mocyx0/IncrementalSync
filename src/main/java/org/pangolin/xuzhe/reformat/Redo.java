package org.pangolin.xuzhe.reformat;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static org.pangolin.xuzhe.reformat.Constants.REDO_NUM;

/**
 * Created by XuZhe on 2017/6/23.
 */
public class Redo extends Thread {
    static Logger logger = LoggerFactory.getLogger(Server.class);
    private static final long firstLevelPKMaxValue = 1200_0000;
    private static byte[][] firstLevelDataStore;
    private static int columnCount;
    private static CountDownLatch latch = new CountDownLatch(1);
    public static void initFirstLevelStore(int columnCount) {
        Redo.columnCount = columnCount;
        firstLevelDataStore = new byte[columnCount][(int)(firstLevelPKMaxValue)*8];
        latch.countDown();
    }
    private final int id;
    private Parser[] parsers;

    private boolean[] firstLevelStoreKeys;
//    private HashLongIntMap storeIndexMap;
    private MyLong2IntHashMap storeIndexMap;
    private byte[][] secondLevelDataStore;
    private int nextStoreIndex;
    private byte[] num2StrBuf = new byte[20];
    public Redo(int id, Parser[] parsers) {
        this.parsers = parsers;
        this.id = id;
        setName("Redo" + id);
        firstLevelStoreKeys = new boolean[(int)firstLevelPKMaxValue];
//        storeIndexMap = HashLongIntMaps.getDefaultFactory().withDefaultValue(-1).withHashConfig(
//                HashConfig.fromLoads(0.8, 0.95, 0.95)).newMutableMap(199_9999);
        storeIndexMap = new MyLong2IntHashMap(199_9999, 0.95f);
        nextStoreIndex = 0;
    }

    @Override
    public void run() {
        try {
            latch.await();
            secondLevelDataStore = new byte[columnCount][(1<<20)*columnCount*8];
            long secondLevelCount = 0;
            int index = 0, cnt = 0;
            long newPk = -1, oldPk = -1;
            int pkChanged = 0;
            while(true) {
                Parser parser = parsers[index%parsers.length];
//                System.out.println(parser.getName() + ", " + parser.blockingQueue[this.id].size());
                ByteArrayPool.ByteArray uncompressedBuffer = parser.blockingQueue[this.id].take();
                byte[] uncompressed = uncompressedBuffer.array;
                ++index;
                if(uncompressed.length == 0) {
                    ++cnt;
                    if(cnt == parsers.length) {
                        logger.info(getName() + ", block Cnt:" + index);
                        logger.info(getName() + ", SecondLevel Cnt:" + secondLevelCount);
                        logger.info(getName() + ", StoreIndexMap.size:" + storeIndexMap.size());
                        break;
                    }
                    continue;
                }
                int blockSize = uncompressedBuffer.dataSize;
                byte[] rawBuf = uncompressed;
                for(int i = 0; i < blockSize; ) {
                    int op = rawBuf[i];
                    if(op == 'I') {
                        newPk = rawBuf[i+1];
                        newPk = (newPk << 8) | (rawBuf[i+2] & 0xFF);
                        newPk = (newPk << 8) | (rawBuf[i+3] & 0xFF);
                        newPk = (newPk << 8) | (rawBuf[i+4] & 0xFF);
                        newPk = (newPk << 8) | (rawBuf[i+5] & 0xFF);
                        newPk = (newPk << 8) | (rawBuf[i+6] & 0xFF);
                        newPk = (newPk << 8) | (rawBuf[i+7] & 0xFF);
                        newPk = (newPk << 8) | (rawBuf[i+8] & 0xFF);
                        i += 9;
                    } else if(op == 'U') {
                        pkChanged = rawBuf[i+1];
                        if(pkChanged == 1) {
                            oldPk = rawBuf[i+2];
                            oldPk = (oldPk << 8) | (rawBuf[i+3] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+4] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+5] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+6] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+7] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+8] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+9] & 0xFF);
                            newPk = rawBuf[i+10];
                            newPk = (newPk << 8) | (rawBuf[i+11] & 0xFF);
                            newPk = (newPk << 8) | (rawBuf[i+12] & 0xFF);
                            newPk = (newPk << 8) | (rawBuf[i+13] & 0xFF);
                            newPk = (newPk << 8) | (rawBuf[i+14] & 0xFF);
                            newPk = (newPk << 8) | (rawBuf[i+15] & 0xFF);
                            newPk = (newPk << 8) | (rawBuf[i+16] & 0xFF);
                            newPk = (newPk << 8) | (rawBuf[i+17] & 0xFF);
                            i += 18;
                        } else {
                            oldPk = rawBuf[i+2];
                            oldPk = (oldPk << 8) | (rawBuf[i+3] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+4] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+5] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+6] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+7] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+8] & 0xFF);
                            oldPk = (oldPk << 8) | (rawBuf[i+9] & 0xFF);
                            newPk = oldPk;
                            i += 10;
                        }
                    } else if(op == 'D') {
                        oldPk = rawBuf[i+1];
                        oldPk = (oldPk << 8) | (rawBuf[i+2] & 0xFF);
                        oldPk = (oldPk << 8) | (rawBuf[i+3] & 0xFF);
                        oldPk = (oldPk << 8) | (rawBuf[i+4] & 0xFF);
                        oldPk = (oldPk << 8) | (rawBuf[i+5] & 0xFF);
                        oldPk = (oldPk << 8) | (rawBuf[i+6] & 0xFF);
                        oldPk = (oldPk << 8) | (rawBuf[i+7] & 0xFF);
                        oldPk = (oldPk << 8) | (rawBuf[i+8] & 0xFF);
                        i += 9;
                    } else {
                        throw new RuntimeException("Op Error, index:" + i + ", op:" + op);
                    }

                    if(op != 'D') {
                        int columnCnt;
                        if(op == 'I') {
                            columnCnt = 4;
                            i = insertRecord(newPk, rawBuf, i );
                        } else {
//                            if(oldPk == 297883679 || newPk == 297883679)
//                                System.out.println("Parser");
                            columnCnt = rawBuf[i++];
                            if(pkChanged == 1) {
                                updatePk(oldPk, newPk);
                            }
                            i = updateRecord(newPk, columnCnt, rawBuf, i);
                        }
                    } else {
                        deleteRecord(oldPk);
                    }
                }
                uncompressedBuffer.release();
            }


        } catch (Exception e) {
            logger.info("", e);
        }
    }

    public int getRecord(int pk, byte[] out, int offset) {
        if(firstLevelStoreKeys[pk]) {
            int posBase = pk << 3;
//            System.out.print(pk);
            int pkStrPos = num2StrBuf.length;
            while (pk > 0) {
                --pkStrPos;
                num2StrBuf[pkStrPos] = (byte) (pk % 10 + '0');
                pk /= 10;
            }
            int len = num2StrBuf.length-pkStrPos;
            System.arraycopy(num2StrBuf, pkStrPos, out, offset, len);
            offset += len;
            for(int i = 0; i < columnCount; i++) {
                byte[] src = firstLevelDataStore[i];
                len = src[posBase];
                out[offset++] = '\t';
                System.arraycopy(src, posBase+1, out, offset, len);
                offset += len;
//                String s = new String(firstLevelDataStore[i], posBase+1, firstLevelDataStore[i][posBase]);
//                System.out.print('\t');
//                System.out.print(s);
//                posBase += 8;
            }
            out[offset++] = '\n';
            return offset;
//            System.out.println();
        } else {
            return -1;
        }
    }

    private void deleteRecord(long pk) {
        if(pk < firstLevelPKMaxValue) {
            firstLevelStoreKeys[(int)pk] = false;
        } /*else {
            storeIndexMap.remove(pk);
        }*/
    }

    private int updateRecord(long pk, int columnCnt, byte[] rawData, int off) {
        if(pk < firstLevelPKMaxValue) {
            int intPk = (int)pk;
            if(!firstLevelStoreKeys[intPk]) {
                return off + (columnCnt<<3);
            }
            int posBase = intPk<<3;
            for (int i = 0; i < columnCnt; i++) {
                int columnNo = rawData[off];
                int valueLen = rawData[off + 1];
//                int pos = posBase + (columnNo << 3);
                byte[] dest = firstLevelDataStore[columnNo];
                dest[posBase] = (byte) valueLen;
                System.arraycopy(rawData, off + 2, dest, posBase + 1, 6);
                off += 8;
            }
        } else {
            int posBase = storeIndexMap.get(pk);
//            int posBase = -1;
            if(posBase == -1) {
                return off + (columnCnt<<3);
            }
            for (int i = 0; i < columnCnt; i++) {
                int columnNo = rawData[off];
                int valueLen = rawData[off + 1];
                byte[] dest = secondLevelDataStore[columnNo];
                dest[posBase] = (byte) valueLen;
                System.arraycopy(rawData, off + 2, dest, posBase+1, valueLen);
                off += 8;
            }
        }
        return off;
    }

    private int updatePk(long oldPk, long newPk) {
        if(oldPk > firstLevelPKMaxValue) { // 旧记录在二级存储中
            int oldSecondLevelPosBase = storeIndexMap.get(oldPk);
//            int oldSecondLevelPosBase = -1;
            if(oldSecondLevelPosBase == -1) { // 不在该线程
                return 0;
            }
            if (newPk < firstLevelPKMaxValue) { // 放入一级存储
//            secondLevelCount--;
                int intPk = (int) newPk;
                firstLevelStoreKeys[intPk] = true; // 标记该主键在一级存储中存在
                int srcPos = oldSecondLevelPosBase;
                int destPos = intPk << 3;
                for(int i = 0; i < columnCount; i++) {
                    byte[] dest = firstLevelDataStore[i];
                    byte[] src = secondLevelDataStore[i];
                    System.arraycopy(src, srcPos, dest, destPos, 8);
//                    srcPos += 8;
                }
            } else {
                storeIndexMap.put(newPk, oldSecondLevelPosBase);
            }
        } else { // 旧记录在一级存储中
            int intOldPk = (int) oldPk;
            if(!firstLevelStoreKeys[intOldPk]) { // 不在该线程
                return 0;
            }
            if (newPk < firstLevelPKMaxValue) { // 从一级存储中更新到一级存储中
                int intNewPk = (int) newPk;
                firstLevelStoreKeys[intNewPk] = true;
                firstLevelStoreKeys[intOldPk] = false;
                for(int i = 0; i < columnCount; i++) {
                    byte[] dest = firstLevelDataStore[i];
                    System.arraycopy(dest, intOldPk<<3, dest, intNewPk<<3, 8);
                }
            } else { // 从一级存储中更新到二级存储中
                int index = nextStoreIndex++;
                firstLevelStoreKeys[intOldPk] = false;
                int srcPos = intOldPk<<3;
                int destPos = index<<3;
                storeIndexMap.put(newPk, destPos);
                for(int i = 0; i < columnCount; i++) {
                    byte[] src = firstLevelDataStore[i];
                    byte[] dest = secondLevelDataStore[i];
                    System.arraycopy(src, srcPos, dest, destPos, 8);
                }
            }
        }
        return 0;
    }

    private int insertRecord(long pk, byte[] rawData, int off) {
        if(pk % REDO_NUM != this.id) {
            return off + (columnCount<<3);
        }
        if(pk < firstLevelPKMaxValue) {
            int intPk = (int)pk;

            firstLevelStoreKeys[intPk] = true;
//            int posBase = intPk*columnCount*8;
            int posBase = intPk<<3;
            for (int i = 0; i < columnCount; i++) {
                int valueLen = rawData[off];
                byte[] dest = firstLevelDataStore[i];
                dest[posBase] = (byte) valueLen;
                System.arraycopy(rawData, off + 1, dest, posBase + 1, 7);
                off += 8;
//                posBase += 8;
            }
        } else {
//            secondLevelCount++;
            int index = nextStoreIndex++;
            int posBase = index<<3;
//            storeIndexMap.put(pk, posBase);
            for (int i = 0; i < columnCount; i++) {
                int valueLen = rawData[off];
                byte[] dest = secondLevelDataStore[i];
                dest[posBase] = (byte) valueLen;
                System.arraycopy(rawData, off+1, dest, posBase+1, 7);
                off += 8;
            }
        }
        return off;
    }

    public static int uncompress( Inflater inf, final byte[] src, int offIn, int lenIn, byte[] out, int offOut, int offLimit) {
        inf.reset();
        int n, cnt = 0;
        inf.setInput(src, offIn, lenIn);
        inf.finished();
        try {
            while ((n = inf.inflate(out, offOut+cnt, offLimit-cnt)) != 0) {
                cnt += n;
                if (inf.finished() || inf.needsDictionary()) {
                    break;
                }
            }
        } catch (DataFormatException e) {
            e.printStackTrace();
            return 0;
        }
        return cnt;
    }

    public static void main(String[] args) throws InterruptedException {
        long begin = System.currentTimeMillis();
        Redo redo = new Redo(1, null);
        redo.start();
        redo.join();
        long end = System.currentTimeMillis();
        System.out.println((end-begin));
    }
}
