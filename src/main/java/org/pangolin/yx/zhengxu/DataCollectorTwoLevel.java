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
    public void writeBuffer(long id, ByteBuffer buffer) throws Exception {
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
            writeLong(buffer, id);

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

    final static char[] DigitTens = {
            '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
            '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
            '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
            '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
            '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
            '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
            '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
            '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
            '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
            '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
    };

    final static char[] DigitOnes = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    };
    final static char[] digits = {
            '0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'a', 'b',
            'c', 'd', 'e', 'f', 'g', 'h',
            'i', 'j', 'k', 'l', 'm', 'n',
            'o', 'p', 'q', 'r', 's', 't',
            'u', 'v', 'w', 'x', 'y', 'z'
    };

    private void writeLong2(ByteBuffer buff, long v) {
        int size = 19;
        long p = 10;
        for (int i = 1; i < 19; i++) {
            if (v < p) {
                size = i;
                break;
            }
            p = 10 * p;
        }
        long q;
        int r;
        long i = v;

        // Get 2 digits/iteration using longs until quotient fits into an int
        while (i > Integer.MAX_VALUE) {
            q = i / 100;
            // really: r = i - (q * 100);
            r = (int) (i - ((q << 6) + (q << 5) + (q << 2)));
            i = q;
            buff.put((byte) DigitOnes[r]);
            buff.put((byte) DigitTens[r]);
        }

        // Get 2 digits/iteration using ints
        int q2;
        int i2 = (int) i;
        while (i2 >= 65536) {
            q2 = i2 / 100;
            // really: r = i2 - (q * 100);
            r = i2 - ((q2 << 6) + (q2 << 5) + (q2 << 2));
            i2 = q2;
            buff.put((byte) DigitOnes[r]);
            buff.put((byte) DigitTens[r]);
        }

        // Fall thru to fast mode for smaller numbers
        // assert(i2 <= 65536, i2);
        for (; ; ) {
            q2 = (i2 * 52429) >>> (16 + 3);
            r = i2 - ((q2 << 3) + (q2 << 1));  // r = i2-(q2*10) ...
            buff.put((byte) digits[r]);
            i2 = q2;
            if (i2 == 0) break;
        }
    }

    private void writeLong(ByteBuffer buff, long v) {
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
    }
}



