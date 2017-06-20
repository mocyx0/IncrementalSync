package org.pangolin.xuzhe.positiveorder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

/**
 * Created by ubuntu on 17-6-19.
 */
public class DataStore {
    private static final int BLOCK_SIZE = 1<<25;
    private static final int STACK_SIZE = 1<<20;
    private final int columnCount;
    //    private ArrayList<byte[]> dataStore = new ArrayList<>();
    private byte[] dataStore;
    private Deque<Integer> deque = new ArrayDeque<>(STACK_SIZE);
    private int nextIndex = 0;
    public DataStore(int columnCount) {
        this.columnCount = columnCount;
        dataStore = new byte[BLOCK_SIZE*columnCount];
    }

    public int getRecord(int indexInStore, byte[] out, int pos) {
        int posInStore = (indexInStore*columnCount) * 8;
        for(int i = 0; i < columnCount; ++i) {
            int len = dataStore[posInStore];
//            posInStore += 1;
            out[pos++] = '\t';
            System.arraycopy(dataStore, posInStore+1, out, pos, len);
            pos += len;
            posInStore += 8;
        }
        out[pos++] = '\n';
        return pos;

    }

    public void updateRecord(int indexInStore, int columnIndex, byte[] dataSrc, int pos, int len) {
        int posInStore = (indexInStore*columnCount + columnIndex) * 8;
        dataStore[posInStore] = (byte)len;
        System.arraycopy(dataSrc, pos, dataStore, posInStore+1, len);
    }

    public int createRecord() {
        Integer integer = deque.pollFirst();
        if(integer == null) {
            return nextIndex++;
        } else {
            return integer;
        }
    }

    public void deleteRecord(int indexInStore) {
        deque.addLast(indexInStore);
    }
}
