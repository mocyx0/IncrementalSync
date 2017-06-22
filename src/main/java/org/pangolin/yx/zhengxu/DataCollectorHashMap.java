package org.pangolin.yx.zhengxu;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/17.
 */
public final class DataCollectorHashMap implements DataCollector {

    ArrayList<DataStorageHashMap> dataStorageHashMaps = new ArrayList<>();

    public DataCollectorHashMap(ArrayList<DataStorage> dataStorages) {
        for (DataStorage dataStorage : dataStorages) {
            dataStorageHashMaps.add((DataStorageHashMap) dataStorage);
        }
    }


    private DataStorageHashMap getDataMap(long id) {
        int block = (int) (id % dataStorageHashMaps.size());
        DataStorageHashMap data = dataStorageHashMaps.get(block);
        return data;
    }

    @Override
    public void writeBuffer(long id, ByteBuffer buffer) throws Exception {
        DataStorageHashMap data = getDataMap(id);
        DataStorageHashMap.Node node = data.getNode(id, -1);
        //node.valid should be true
        if (node != null && node.valid) {
            byte[] bytes = new byte[node.bytes.length];
            System.arraycopy(node.bytes, 0, bytes, 0, node.bytes.length);
            int colCount = 0;
            for (int i = 0; i < data.CELL_COUNT; i++) {
                int dataPos = i * data.CELL_SIZE;
                if (bytes[dataPos] != -1) {
                    colCount++;
                }
            }
            //注意preId和lastSeq的更新
            long preId = node.preid;
            long lastSeq = node.seq;
            while (colCount != data.CELL_COUNT && preId != -1) {
                //获取新的dataMao
                data = getDataMap(preId);
                DataStorageHashMap.Node preNode = data.getNode(preId, lastSeq);
                for (int i = 0; i < data.CELL_COUNT; i++) {
                    int dataPos = i * data.CELL_SIZE;
                    if (preNode.bytes[dataPos] != -1 && bytes[dataPos] == -1) {
                        int len = preNode.bytes[dataPos];
                        System.arraycopy(preNode.bytes, dataPos, bytes, dataPos, len + 1);
                        colCount++;
                    }
                }
                preId = preNode.preid;
                lastSeq = preNode.seq;
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
    }
}
