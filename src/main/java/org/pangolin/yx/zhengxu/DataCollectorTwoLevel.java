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
        datas = new long[GlobalData.colCount];
    }

    private DataStorageTwoLevel getDataMap(long id) {
        int block = (int) (id % dataStorageHashMaps.size());
        DataStorageTwoLevel data = dataStorageHashMaps.get(block);
        return data;
    }

    long[] datas;

    @Override
    public void writeBuffer(long id, ByteBuffer buffer, byte[] pkBuff) throws Exception {
        for (int i = 0; i < datas.length; i++) {
            datas[i] = 0;
        }
        DataStorageTwoLevel data = getDataMap(id);
        RecordData recordData = data.getRecord(id, -1);
        if (recordData != null) {
            //System.arraycopy(recordData.colData, 0, datas, 0, datas.length);
            for (int i = 0; i < datas.length; i++) {
                datas[i] = recordData.colData[i];
            }

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
            //buffer.put(Long.toString(id).getBytes());
            writeLong(buffer, id, pkBuff);

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


        /*
        int size = 19;
        long p = 10;
        for (int i = 1; i < 19; i++) {
            if (v < p) {
                size = i;
                break;
            }
            p = 10 * p;
        }
        for (int i = size - 1; i >= 0; i--) {
            p = p / 10;
            long k = v / p;
            buff.put((byte) ((k) + '0'));
            v -= k * p;
        }
        */
    }
}



