package org.pangolin.xuzhe.positiveorder;

import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.map.hash.HashLongIntMap;
import com.koloboke.collect.map.hash.HashLongIntMaps;

import static org.pangolin.xuzhe.positiveorder.Constants.REDO_NUM;

/**
 * Created by XuZhe on 2017/6/24.
 */
public class SplitedItemDataStore {
    private HashLongIntMap storeIndexMap;
    private final int columnCount;
    private int id;
    private byte[] firstLevelDataStore;
    private int nextIndex = 0;
    public SplitedItemDataStore(int columnCount, int id) {
        this.id = id;
        this.columnCount = columnCount;
        storeIndexMap = HashLongIntMaps.getDefaultFactory().withDefaultValue(-1).withHashConfig(
                HashConfig.fromLoads(0.8, 0.95, 0.95)).newMutableMap(150_0000);
        firstLevelDataStore = new byte[1<<26];
    }

    public void insertRecord(long pk, int[] newValues, int[] valueLen, byte[] src) {
        int storeBeginIndex = nextIndex;
        nextIndex += columnCount<<3;
        int beginIndex;
        for(int i = 0; i < columnCount; i++) {
            beginIndex = storeBeginIndex+(i<<3);
            int len = valueLen[i];
            firstLevelDataStore[beginIndex] = (byte)len;
            System.arraycopy(src, newValues[i], firstLevelDataStore, beginIndex+1, len);
        }
        storeIndexMap.put(pk, storeBeginIndex);
    }

    public void updateRecord(long pk, int columnSize, int[] columnNos, int[] newValues, int[] valueLen, byte[] src) {
        int storeBeginIndex = storeIndexMap.get(pk);
        int beginIndex;
        for(int i = 0; i < columnSize; i++) {
            int columnNo = columnNos[i];
            beginIndex = storeBeginIndex+(columnNo<<3);
            int len = valueLen[i];
            firstLevelDataStore[beginIndex] = (byte)len;
            System.arraycopy(src, newValues[i], firstLevelDataStore, beginIndex+1, len);
        }
    }

    public void updatePK(long oldPk, long newPk) {
        int storeBeginIndex = storeIndexMap.remove(oldPk);
        storeIndexMap.put(newPk, storeBeginIndex);
    }

    public void deleteRecord(long pk) {
        storeIndexMap.remove(pk);
    }

    public void process(LogIndex logIndex) {
        byte[] dataSrc = logIndex.getByteBuffer().array();
        long[] oldPKs = logIndex.getOldPks();
        int logSize = logIndex.getLogSize();
        for (int i = 0; i < logSize; i++) {
            long oldPK = oldPKs[i];
            if (oldPK != -1) { // 更新或删除
                long storeBeginIndex = storeIndexMap.get(oldPK);
                if (storeBeginIndex == -1) {
                    continue;
                }
                int logType = logIndex.getLogType(i);
                if (logType == 'U') { //更新
                    long newPk = logIndex.getNewPk(i);
                    if (newPk != oldPK) {
                        updatePK(oldPK, newPk);
                    }
                    int columnSize = logIndex.getColumnSize(i);
                    int[] newValues = logIndex.getColumnNewValues(i);
                    int[] names = logIndex.getHashColumnName(i);
                    int[] valueLens = logIndex.getColumnValueLens(i);
                    updateRecord(newPk, columnSize, names, newValues, valueLens, dataSrc);
                } else {
                    deleteRecord(oldPK);
                }
            } else { // 插入操作
                long newPk = logIndex.getNewPk(i);
                if (newPk % REDO_NUM == this.id) { // 记录属于该线程，执行插入
                    int[] newValues = logIndex.getColumnNewValues(i);
                    int[] names = logIndex.getHashColumnName(i);
                    int[] valueLens = logIndex.getColumnValueLens(i);
                    insertRecord(newPk, newValues, valueLens, dataSrc);
                }
            }
        }
    }

    public int getRecord(long pk, byte[] out, int pos) {
        return -1;
    }
}
