package org.pangolin.xuzhe.positiveorder;

import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.map.hash.HashLongIntMap;
import com.koloboke.collect.map.hash.HashLongIntMapFactory;
import com.koloboke.collect.map.hash.HashLongIntMaps;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

import static org.pangolin.xuzhe.positiveorder.Constants.REDO_NUM;

/**
 * Created by ubuntu on 17-6-19.
 */
public class DataStore {
    private int[] bitMaps;
    static private final long bitIndexMaxValue = 1<<24; // 16777216
    private static final long FIRST_LEVEL_MAX_KEY = bitIndexMaxValue;
    private static final int FIRST_LEVEL_BLOCK_SIZE = 1<<24;
    private static final int SECOND_LEVEL_BLOCK_SIZE_BIT = 21;
    private static final int SECOND_LEVEL_BLOCK_SIZE = 1<<SECOND_LEVEL_BLOCK_SIZE_BIT;
    private static final int STACK_SIZE = 1<<20;
    private final int columnCount;
    //    private ArrayList<byte[]> firstLevelDataStore = new ArrayList<>();
    private byte[] firstLevelDataStore;
    private boolean[] firstLevelStoreKeys;
    private ArrayList<byte[]> secondLevelDataStore;
    private Deque<Integer> deque = new ArrayDeque<>(STACK_SIZE);
    private HashLongIntMap storeIndexMap;
    private int nextIndexInSecondLevel = 0;
    private int id;
    /**
     * 一条记录占用的字节数
     */
    private final int allColumnByteLength;


    public DataStore(int columnCount, int id) {
        this.columnCount = columnCount;
        allColumnByteLength = columnCount<<3;
        firstLevelDataStore = new byte[FIRST_LEVEL_BLOCK_SIZE *columnCount];
        firstLevelStoreKeys = new boolean[5000_0000/ REDO_NUM];
        secondLevelDataStore = new ArrayList<>();
        secondLevelDataStore.add(new byte[SECOND_LEVEL_BLOCK_SIZE]);
//        storeIndexMap = new MyLong2IntHashMap(2000000, 0.95f);
        this.id = id;
        storeIndexMap = HashLongIntMaps.getDefaultFactory().withDefaultValue(-1).withHashConfig(
                HashConfig.fromLoads(0.8, 0.95, 0.95)).newMutableMap(100_0000);
        bitMaps = new int[(int)(bitIndexMaxValue>>5)];
    }


    public void process(LogIndex logIndex) {
        byte[] dataSrc = logIndex.getByteBuffer().array();
        long[] oldPKs = logIndex.getOldPks();
        int logSize = logIndex.getLogSize();
        for (int i = 0; i < logSize; i++) {
            long oldPK = oldPKs[i];
            if(oldPK != -1) { // 更新或删除
                if(oldPK < FIRST_LEVEL_MAX_KEY) { // 旧主键对应的记录在位图索引中有记录
                    int intOldPK = (int) oldPK;
                    if (bitScan(intOldPK) == 1) { // 在该线程中
                        int logType = logIndex.getLogType(i);
                        if (logType == 'U') { //更新
                            long newPk = logIndex.getNewPk(i);
                            int intNewPk = (int) newPk;
                            if(newPk != oldPK) {
                                updateRecordPK(oldPK, newPk);
                            }
                            int columnSize = logIndex.getColumnSize(i);
                            int[] newValues = logIndex.getColumnNewValues(i);
                            int[] names = logIndex.getHashColumnName(i);
                            int[] valueLens = logIndex.getColumnValueLens(i);
                            if(columnSize != 0) {
                                if (intNewPk % REDO_NUM == this.id) { // 在一级存储中
                                    int posBaseInFirstLevelStore = (intNewPk / REDO_NUM) * allColumnByteLength;
                                    for (int j = 0; j < columnSize; j++) {
                                        int columnIndex = names[j];
                                        int columnPos = newValues[j];
                                        int columnLen = valueLens[j];
                                        updateFirstLevelStore(posBaseInFirstLevelStore, columnIndex, dataSrc, columnPos, columnLen);
                                    }
                                } else { // 在二级存储中
                                    int indexInSecondLevelStore = storeIndexMap.get(newPk);
                                    for (int j = 0; j < columnSize; j++) {
                                        int columnIndex = names[j];
                                        int columnPos = newValues[j];
                                        int columnLen = valueLens[j];
                                        // TODO 有待优化
                                        updateSecondLevelStore(indexInSecondLevelStore, columnIndex, dataSrc, columnPos, columnLen);
                                    }
                                }
                            }
                        } else { // 删除操作
                            deleteRecord(oldPK);
                        }
                    } else { // 不在该线程 直接忽略

                    }
                }
            } else { // 插入操作
                long newPk = logIndex.getNewPk(i);
                if(newPk % REDO_NUM == this.id) { // 记录属于该线程，执行插入
                    int columnSize = columnCount;
                    int[] newValues = logIndex.getColumnNewValues(i);
                    int[] names = logIndex.getHashColumnName(i);
                    int[] valueLens = logIndex.getColumnValueLens(i);
                    if(newPk < FIRST_LEVEL_MAX_KEY) {
                        int intNewPk = (int) newPk;
                        int indexInFirstLevelStore = (intNewPk / REDO_NUM);
                        int posBaseInFirstLevelStore = indexInFirstLevelStore * allColumnByteLength;
                        insertRecordToFirstLevel(intNewPk, indexInFirstLevelStore);
                        for (int j = 0; j < columnSize; j++) {
                            int columnIndex = names[j];
                            int columnPos = newValues[j];
                            int columnLen = valueLens[j];
                            updateFirstLevelStore(posBaseInFirstLevelStore, columnIndex, dataSrc, columnPos, columnLen);
                        }
                    } else {
                        int indexInSecondLevelStore = insertRecordToSecondLevel(newPk);
                        for (int j = 0; j < columnSize; j++) {
                            int columnIndex = names[j];
                            int columnPos = newValues[j];
                            int columnLen = valueLens[j];
                            // TODO 有待优化
                            updateSecondLevelStore(indexInSecondLevelStore, columnIndex, dataSrc, columnPos, columnLen);
                        }
                    }

                } else { // 记录不属于该线程
//                    continue;
                }
            }
        }
    }

    /**
     * 在一级存储的位图索引中检索，记录是否存在
     * @param id
     * @return
     */
    public boolean exist(int id) {
        return bitScan(id) == 1;
    }

    /**
     *
     * @param pk
     * @param out
     * @param pos
     * @return 记录占用的字节数。若该存储引擎无对应记录，返回-1
     */
    public int getRecord(long pk, byte[] out, int pos) {
        if(pk < FIRST_LEVEL_MAX_KEY) {
            int intPk = (int)pk;
            if(bitScan(intPk) == 1) {
                if(intPk % REDO_NUM == this.id) {
                    int posBaseInFirstLevelStore = (intPk / REDO_NUM) * allColumnByteLength ;
                    for(int i = 0; i < columnCount; ++i) {
                        int len = firstLevelDataStore[posBaseInFirstLevelStore];
                        out[pos++] = '\t';
                        System.arraycopy(firstLevelDataStore, posBaseInFirstLevelStore+1, out, pos, len);
                        pos += len;
                        posBaseInFirstLevelStore += 8;
                    }
                    out[pos++] = '\n';
                    return pos;
                } else { // 在二级存储中
                    int indexInSecondLevelStore = storeIndexMap.get(pk);
                    int posInStore = indexInSecondLevelStore*allColumnByteLength;
                    int blockIndex = posInStore >> (SECOND_LEVEL_BLOCK_SIZE_BIT);
                    int posInBlock = posInStore & (SECOND_LEVEL_BLOCK_SIZE-1);
                    byte[] block = secondLevelDataStore.get(blockIndex);
                    for(int i = 0; i < columnCount; ++i) {
                        byte len = block[posInBlock];
                        out[pos++] = '\t';
                        System.arraycopy(block, posInBlock+1, out, pos, len);
                        pos += len;
                        posInBlock += 8;
                    }
                    out[pos++] = '\n';
                    return pos;
                }
            } else {
                return -1;
            }
        } else {
            int indexInSecondLevelStore = storeIndexMap.get(pk);
            if(indexInSecondLevelStore == -1) {
                return -1;
            }
            int posInStore = indexInSecondLevelStore*allColumnByteLength;
            int blockIndex = posInStore >> (SECOND_LEVEL_BLOCK_SIZE_BIT);
            int posInBlock = posInStore & (SECOND_LEVEL_BLOCK_SIZE-1);
            byte[] block = secondLevelDataStore.get(blockIndex);
            for(int i = 0; i < columnCount; ++i) {
                byte len = block[posInBlock];
                out[pos++] = '\t';
                System.arraycopy(block, posInBlock+1, out, pos, len);
                pos += len;
                posInBlock += 8;
            }
            out[pos++] = '\n';
            return pos;
        }
    }


    /**
     * 确认oldPK属于本存储，该方法保证应该放入一级存储中的记录，变更主键后依然在一级存储中
     * @param oldPK
     * @param newPK
     */
    public void updateRecordPK(long oldPK, long newPK) {
        if(oldPK < FIRST_LEVEL_MAX_KEY) {
            int intOldPK = (int)oldPK;
            int indexInFirstLevelStore = intOldPK / REDO_NUM;
            bitUpdate(intOldPK, false);
            if(intOldPK % REDO_NUM == id) { // 对应记录在一级存储中
                int intNewPK = (int)newPK;
                if(newPK < FIRST_LEVEL_MAX_KEY && intNewPK % REDO_NUM == id) { // 对应记录应放入一级存储
                    bitUpdate(intNewPK, true);
                    int newDataPos = intNewPK / REDO_NUM;
                    firstLevelStoreKeys[indexInFirstLevelStore] = false;
                    firstLevelStoreKeys[newDataPos] = true;
                    System.arraycopy(firstLevelDataStore, indexInFirstLevelStore * allColumnByteLength,
                            firstLevelDataStore, newDataPos * allColumnByteLength, allColumnByteLength);
                } else {
                    int indexInSecondLevelStore = insertRecordToSecondLevel(newPK);
                    int blockIndex = indexInSecondLevelStore >> (SECOND_LEVEL_BLOCK_SIZE_BIT);
                    int posInBlock = indexInSecondLevelStore & (SECOND_LEVEL_BLOCK_SIZE-1);
                    byte[] block = secondLevelDataStore.get(blockIndex);
                    firstLevelStoreKeys[indexInFirstLevelStore] = false;
                    storeIndexMap.put(newPK, indexInSecondLevelStore);
                    System.arraycopy(firstLevelDataStore, indexInFirstLevelStore * allColumnByteLength,
                            block, posInBlock, allColumnByteLength);
                }
            } else { // 对应记录在二级存储中
                int intNewPK = (int)newPK;
                if(newPK < FIRST_LEVEL_MAX_KEY && intNewPK % REDO_NUM == id) { // 对应记录应放入一级存储
                    bitUpdate(intNewPK, true);
                    int newDataPos = intNewPK / REDO_NUM;
                    firstLevelStoreKeys[indexInFirstLevelStore] = false;
                    firstLevelStoreKeys[newDataPos] = true;
                    System.arraycopy(firstLevelDataStore, indexInFirstLevelStore * allColumnByteLength,
                            firstLevelDataStore, newDataPos * allColumnByteLength, allColumnByteLength);
                } else {
                    int pos = storeIndexMap.get(oldPK);
                    storeIndexMap.put(newPK, pos);
                }
            }


        } else { // 对应记录在二级存储中
            int oldIndexInSecondLevelStore = storeIndexMap.remove(oldPK);
            if(newPK >= FIRST_LEVEL_MAX_KEY) {
                storeIndexMap.put(newPK, oldIndexInSecondLevelStore);
            } else {
                int intNewPK = (int)newPK;
                if(intNewPK % REDO_NUM == id) { // 对应记录应放入一级存储
                    int oldPosInStore = (oldIndexInSecondLevelStore*allColumnByteLength)<<3;
                    int oldBlockIndex = oldPosInStore >> (SECOND_LEVEL_BLOCK_SIZE_BIT);
                    int oldPosInBlock = oldPosInStore & (SECOND_LEVEL_BLOCK_SIZE-1);
                    byte[] block = secondLevelDataStore.get(oldBlockIndex);
                    int indexInFirstLevelStore = intNewPK / REDO_NUM;
                    int posInStore = indexInFirstLevelStore*allColumnByteLength;
                    firstLevelStoreKeys[indexInFirstLevelStore] = true;
                    System.arraycopy(block, oldPosInBlock, firstLevelDataStore, posInStore, allColumnByteLength);
                } else { // 对应记录应放入二级存储
                    storeIndexMap.put(newPK, oldIndexInSecondLevelStore);
                }
            }
        }
    }

    /**
     * 先通过位图索引，判断一级存储中没有，在二级索引中有对应记录时再调用该函数
     * @param indexInSecondLevelStore  位置编号，非数据基地址
     * @param columnIndex
     * @param dataSrc
     * @param pos
     * @param len
     */
    public void updateSecondLevelStore(int indexInSecondLevelStore, int columnIndex, byte[] dataSrc, int pos, int len) {
//        int indexInSecondLevelStore = storeIndexMap.get(pk);
        int posInStore = (indexInSecondLevelStore*columnCount+columnIndex) << 3;
        int blockIndex = posInStore >> (SECOND_LEVEL_BLOCK_SIZE_BIT);
        int posInBlock = posInStore & (SECOND_LEVEL_BLOCK_SIZE-1);
        byte[] block = secondLevelDataStore.get(blockIndex);
        block[posInBlock] = (byte)len;
        System.arraycopy(dataSrc, pos, block, posInBlock+1, len);
    }


    /**
     * 在调用该方法前需要确定该条记录确实在本存储中
     * @param posBaseInFirstLevelStore  对应记录在一级存储中的基地址
     * @param columnIndex  第几列
     * @param dataSrc
     * @param pos
     * @param len
     */
    public void updateFirstLevelStore(int posBaseInFirstLevelStore, int columnIndex, byte[] dataSrc, int pos, int len) {
//        if(pk < FIRST_LEVEL_MAX_KEY) {
//            int intPK = (int)pk;
//            int indexInFirstLevelStore = intPK / REDO_NUM;
//            if(intPK / REDO_NUM == ) {
                int posInStore = posBaseInFirstLevelStore + (columnIndex << 3);
                firstLevelDataStore[posInStore] = (byte)len;
                System.arraycopy(dataSrc, pos, firstLevelDataStore, posInStore+1, len);
//            } else {
//                updateSecondLevelStore(pk, columnIndex, dataSrc, pos, len);
//            }
//        } else {
//            updateSecondLevelStore(pk, columnIndex, dataSrc, pos, len);
//        }
    }

    public void insertRecordToFirstLevel(int newPK, int indexInFirstLevelStore) {
//        int indexInFirstLevelStore = newPK / REDO_NUM;
        bitUpdate(newPK, true);
        firstLevelStoreKeys[indexInFirstLevelStore] = true;
    }


    /**
     *
     * @param newPK
     * @return 返回在二级存储中的整体索引
     */
    public int insertRecordToSecondLevel(long newPK) {
        int indexInSecondLevelStore = nextIndexInSecondLevel;
        int posInStore = (indexInSecondLevelStore*columnCount) << 3;
        int blockIndex = posInStore >> (SECOND_LEVEL_BLOCK_SIZE_BIT);
        if (blockIndex > secondLevelDataStore.size()) {
            secondLevelDataStore.add(new byte[SECOND_LEVEL_BLOCK_SIZE]);
        }
        storeIndexMap.put(newPK, indexInSecondLevelStore);
        ++nextIndexInSecondLevel;
        return indexInSecondLevelStore;
    }

    public void deleteRecord(long oldPK) {
        if(oldPK < FIRST_LEVEL_MAX_KEY) {
            int intPK = (int)oldPK;
            bitUpdate(intPK, false);
            int indexInFirstLevelStore = intPK / REDO_NUM;
            if(indexInFirstLevelStore % REDO_NUM == id) {
                firstLevelStoreKeys[indexInFirstLevelStore] = false;
            } else {
                storeIndexMap.remove(oldPK);
            }
        } else {
            storeIndexMap.remove(oldPK);
        }
    }

    private int bitScan(int key) {
        --key;
        int blockNumber = key >> 5;
        int curpos = key & 0x1F;
        int value = bitMaps[blockNumber];
        int bitValue = (value >> curpos) & 0x01;
        return bitValue;
    }

    public void bitUpdate(int key, boolean bitFlag) {
        --key;
        int blockNumber = key >> 5;
        int curpos = key & 0x1F;
        int value = bitMaps[blockNumber];
        if(bitFlag == true){
            value = value | (1 << curpos);
        }else{
            value = value ^ (1 << curpos);
        }

        bitMaps[blockNumber] = value;
    }
}
