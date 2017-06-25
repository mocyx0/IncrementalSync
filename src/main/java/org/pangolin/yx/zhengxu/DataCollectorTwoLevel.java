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
            long[] datas = new long[recordData.colData.length];
            System.arraycopy(recordData.colData, 0, datas, 0, datas.length);
            int colCount = 0;
            for (int i = 0; i < data.CELL_COUNT; i++) {
                int dataPos = i * data.CELL_SIZE;
                if ((datas[dataPos] & 0xff) != 0) {
                    colCount++;
                }
            }
            long lastSeq = recordData.seq;
            long preId = recordData.preid;
            while (colCount != data.CELL_COUNT && preId != -1) {
                data = getDataMap(preId);
                RecordData preRecord = data.getRecord(preId, lastSeq);
                for (int i = 0; i < data.CELL_COUNT; i++) {
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
            buffer.put(Long.toString(id).getBytes());
            buffer.put((byte) '\t');
            for (int i = 0; i < data.CELL_COUNT; i++) {
                long v = datas[i];
                int len = (int) (v & 0xff);
                for (int j = len - 1; j >= 0; j--) {
                    buffer.put((byte) (v >> (8 + j * 8) & 0xff));
                }
                //buffer.put(datas, dataPos + 1, len);
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



