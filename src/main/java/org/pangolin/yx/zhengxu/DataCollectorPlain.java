package org.pangolin.yx.zhengxu;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/20.
 */
public class DataCollectorPlain implements DataCollector {
    ArrayList<DataStoragePlain> dataStorageHashMaps = new ArrayList<>();

    public DataCollectorPlain(ArrayList<DataStorage> dataStorages) {
        for (DataStorage dataStorage : dataStorages) {
            dataStorageHashMaps.add((DataStoragePlain) dataStorage);
        }
    }

    private DataStoragePlain getDataMap(long id) {
        int block = (int) (id % dataStorageHashMaps.size());
        DataStoragePlain data = dataStorageHashMaps.get(block);
        return data;
    }

    @Override
    public void writeBuffer(long id, ByteBuffer buffer) throws Exception {

        DataStoragePlain data = getDataMap(id);
        int node = data.getNode(id, -1);
        if (node == 0) {
            return;
        }
        //node.valid should be true
        byte valid = data.getValid(node);
        if (valid == 1) {
            return;
        }
        byte[] bytes = new byte[data.CELL_COUNT * data.CELL_SIZE];
        //System.arraycopy(node.bytes, 0, bytes, 0, node.bytes.length);
        data.readBytes(node, 0, bytes, 0, bytes.length);
        int colCount = 0;
        for (int i = 0; i < data.CELL_COUNT; i++) {
            int dataPos = i * data.CELL_SIZE;
            if (bytes[dataPos] != -1) {
                colCount++;
            }
        }
        //注意preId和lastSeq的更新
        long preId = data.getPreid(node);
        long lastSeq = data.getSeq(node);
        while (colCount != data.CELL_COUNT && preId != -1) {
            //获取新的dataMao
            data = getDataMap(preId);
            //DataStorageHashMap.Node preNode = data.getNode(preId, lastSeq);
            int preNode = data.getNode(preId, lastSeq);
            for (int i = 0; i < data.CELL_COUNT; i++) {
                byte[] preData = new byte[bytes.length];
                data.readBytes(preNode, 0, preData, 0, bytes.length);
                int dataPos = i * data.CELL_SIZE;
                if (preData[dataPos] != -1 && bytes[dataPos] == -1) {
                    int len = preData[dataPos];
                    System.arraycopy(preData, dataPos, bytes, dataPos, len + 1);
                    colCount++;
                }
            }
            preId = data.getPreid(preNode);
            lastSeq = data.getSeq(preNode);
            //preId = preNode.preid;
            //lastSeq = preNode.seq;
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
