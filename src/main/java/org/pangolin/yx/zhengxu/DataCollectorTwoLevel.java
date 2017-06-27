package org.pangolin.yx.zhengxu;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created by yangxiao on 2017/6/24.
 */
public class DataCollectorTwoLevel implements DataCollector {
    private ArrayList<DataStorageTwoLevel> dataStorageHashMaps = new ArrayList<>();
    private DataStorageTwoLevel dataStorageTwoLevel;

    private final int cellCount;
    private final int cellSize;

    DataCollectorTwoLevel(ArrayList<DataStorage> dataStorages) {
        for (DataStorage dataStorage : dataStorages) {
            dataStorageHashMaps.add((DataStorageTwoLevel) dataStorage);
        }
        datas = new long[GlobalData.colCount];
        dataStorageTwoLevel = dataStorageHashMaps.get(0);

        cellCount = DataStorageTwoLevel.CELL_COUNT;
        cellSize = DataStorageTwoLevel.CELL_SIZE;
    }


    private DataStorageTwoLevel getDataMap(long id) {
        int block = (int) (id % dataStorageHashMaps.size());
        DataStorageTwoLevel data = dataStorageHashMaps.get(block);
        return data;
    }

    private long[] datas;

    @Override
    public void writeBuffer(long id, ByteBuffer buffer, byte[] pkBuff) throws Exception {
        for (int i = 0; i < datas.length; i++) {
            datas[i] = 0;
        }
        DataStorageTwoLevel data = dataStorageTwoLevel;

        RecordData recordData = data.getRecord(id, -1);
        if (recordData != null) {
            //System.arraycopy(recordData.colData, 0, datas, 0, datas.length);
            for (int i = 0; i < datas.length; i++) {
                datas[i] = recordData.colData[i];
            }

            int colCount = 0;
            for (int i = 0; i < cellCount; i++) {
                int dataPos = i * cellSize;
                if ((datas[dataPos] & 0xff) != 0) {
                    colCount++;
                }
            }
            long lastSeq = recordData.seq;
            long preId = recordData.preid;
            while (colCount != cellCount && preId != -1) {
                //data = getDataMap(preId);
                RecordData preRecord = data.getRecord(preId, lastSeq);
                for (int i = 0; i < cellCount; i++) {
                    if ((datas[i] & 0xff) == 0 && (preRecord.colData[i] & 0xff) != 0) {
                        //long len = preRecord.colData[i] & 0xff;
                        //System.arraycopy(preRecord.colData, dataPos, datas, dataPos, len + 1);
                        datas[i] = preRecord.colData[i];
                        colCount++;
                    }
                }
                preId = preRecord.preid;
                lastSeq = preRecord.seq;
            }
            //finished
            //buffer.put(Long.toString(id).getBytes());
            writeLong(buffer, id, pkBuff);

            buffer.put((byte) '\t');
            for (int i = 0; i < cellCount; i++) {
                long v = datas[i];
                int len = (int) (v & 0xff);
                for (int j = len - 1; j >= 0; j--) {
                    buffer.put((byte) (v >> (8 + j * 8) & 0xff));
                }
                //buffer.put(datas, dataPos + 1, len);
                if (i != cellCount - 1) {
                    buffer.put((byte) '\t');
                }
            }
            buffer.put((byte) '\n');
        }
        /*
        int node = data.getNode(id, -1);
        if (node == 0) {
            return;
        }
        */
    }

    private void writeLong(ByteBuffer buff, long v, byte[] pkBuffer) {
        if (v == 0) {
            buff.put((byte) '0');
        } else {
            int len = 0;
            while (v != 0) {
                pkBuffer[len++] = (byte) (v % 10 + '0');
                v /= 10;
            }
            for (int i = len - 1; i >= 0; i--) {
                buff.put(pkBuffer[i]);
            }
        }
    }
}



