package org.pangolin.yx.zhengxu;

import org.pangolin.yx.Config;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Created by yangxiao on 2017/6/17.
 */
public class DataStorageHashMap implements DataStorage {

    TableInfo tableInfo;

    public int CELL_SIZE;
    public int CELL_COUNT;

    DataStorageHashMap(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        CELL_SIZE = Config.MAX_COL_SIZE + 1;
        CELL_COUNT = tableInfo.columnName.length - 1;
    }

    public static class Node {
        Node next;
        long seq;
        long preid;
        boolean valid = true;
        byte[] bytes;
    }

    //seq=-1 表示不关心seq
    public Node getNode(long id, long seq) {
        if (data.containsKey(id)) {
            Node node = data.get(id);
            if (seq == -1) {
                return node;
            } else {
                while (node != null && node.seq >= seq) {
                    node = node.next;
                }
                return node;
            }
        } else {
            return null;
        }
    }

    HashMap<Long, Node> data = new HashMap<>();

    private void writeToBytes(byte[] bytes, LogRecord logRecord) {
        short[] logData = logRecord.columnData;
        int size = logRecord.columnData.length / 3;
        for (int i = 0; i < size; i++) {
            int index = logData[i * 3];
            if (index == 0) {
                break;
            } else {
                int pos = logData[i * 3 + 1];
                int len = logData[i * 3 + 2];
                int writePos = (index - 1) * CELL_SIZE;
                bytes[writePos] = (byte) len;
                //System.arraycopy(logRecord.lineData, pos, bytes, writePos + 1, len);
                for (int j = 0; j < len; j++) {
                    bytes[writePos + 1 + j] = logRecord.lineData[pos + j];
                }
            }
        }
    }

    private Node createNode() {
        Node node = new Node();
        node.bytes = new byte[CELL_COUNT * CELL_SIZE];
        for (int i = 0; i < CELL_COUNT; i++) {
            node.bytes[i * CELL_SIZE] = -1;//-1表示没有数据
        }
        return node;
    }

    @Override
    public void doLog(LogRecord logRecord) throws Exception {
        if (logRecord.opType == 'U') {
            long id = logRecord.id;
            if (logRecord.preId != logRecord.id) {
                Node next = null;
                if (data.containsKey(id)) {
                    next = data.get(id);
                }
                Node newNode = createNode();
                newNode.seq = logRecord.seq;
                newNode.next = next;
                writeToBytes(newNode.bytes, logRecord);
                newNode.preid = logRecord.preId;
                data.put(id, newNode);
            } else {
                if (!data.containsKey(id)) {
                    throw new Exception("no preid in data");
                }
                Node node = data.get(id);
                writeToBytes(node.bytes, logRecord);
            }
        } else if (logRecord.opType == 'I') {
            Node next = null;
            long id = logRecord.id;
            if (data.containsKey(id)) {
                next = data.get(id);
            }
            Node newNode = createNode();
            newNode.seq = logRecord.seq;
            newNode.next = next;
            writeToBytes(newNode.bytes, logRecord);
            newNode.preid = -1;
            data.put(id, newNode);
        } else if (logRecord.opType == 'D') {
            long id = logRecord.preId;
            if (data.containsKey(id)) {
                Node node = data.get(id);
                if (node.next == null) {
                    data.remove(id);
                } else {
                    data.put(id, node.next);
                }
            }
        } else if (logRecord.opType == 'X') {
            long id = logRecord.id;
            Node node = data.get(id);
            node.valid = false;
        }
    }

}
