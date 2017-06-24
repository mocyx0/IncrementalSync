package org.pangolin.yx.zhengxu;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/24.
 */
public class DataCollectorTwoLevel implements DataCollector {
    ArrayList<DataStorageTwoLevel> dataStorageHashMaps = new ArrayList<>();

    DataCollectorTwoLevel(ArrayList<DataStorage> dataStorages) {
        for (DataStorage dataStorage : dataStorages) {
            dataStorageHashMaps.add((DataStorageTwoLevel) dataStorage);
        }
    }

    private DataStorageTwoLevel getDataMap(long id) {
        int block = (int) (id % dataStorageHashMaps.size());
        DataStorageTwoLevel data = dataStorageHashMaps.get(block);
        return data;
    }

    @Override
    public void writeBuffer(long id, ByteBuffer buffer) throws Exception {

        DataStorageTwoLevel data = getDataMap(id);
        RecordData recordData = data.getRecord(id, -1);
        if (recordData != null) {
            byte[] bytes = new byte[recordData.colData.length];
            System.arraycopy(recordData.colData, 0, bytes, 0, bytes.length);
            int colCount = 0;
            for (int i = 0; i < data.CELL_COUNT; i++) {
                int dataPos = i * data.CELL_SIZE;
                if (bytes[dataPos] != 0) {
                    colCount++;
                }
            }
            long lastSeq = recordData.seq;
            long preId = recordData.preid;
            while (colCount != data.CELL_COUNT && preId != -1) {
                data = getDataMap(preId);
                RecordData preRecord = data.getRecord(preId, lastSeq);
                for (int i = 0; i < data.CELL_COUNT; i++) {
                    int dataPos = i * data.CELL_SIZE;
                    if (preRecord.colData[dataPos] != 0 && bytes[dataPos] == 0) {
                        int len = preRecord.colData[dataPos];
                        System.arraycopy(preRecord.colData, dataPos, bytes, dataPos, len + 1);
                        colCount++;
                    }
                }
                preId = preRecord.preid;
                lastSeq = preRecord.seq;
            }
            //finished
            buffer.put(Long.toString(id).getBytes());
            buffer.put((byte) '\t');
            for (int i = 0; i < data.CELL_COUNT; i++) {
                int dataPos = i * data.CELL_SIZE;
                int len = bytes[dataPos];
                buffer.put(bytes, dataPos + 1, len);
                if (i != data.CELL_COUNT - 1) {
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
}



