package org.pangolin.xuzhe.positiveorder;

import java.util.concurrent.atomic.AtomicLong;

import static org.pangolin.xuzhe.positiveorder.Constants.REDO_NUM;

/**
 * Created by XuZhe on 2017/6/21.
 */
final public class MyLong2IntWithBitIndexHashMap extends MyLong2IntHashMap {
    private int[] bitMaps;
    public static AtomicLong counter = new AtomicLong(0);
    static private final long bitIndexMaxValue = 1<<24; // 16777216
    private int indexForNum;
    public MyLong2IntWithBitIndexHashMap(int initialCapacityBitCount, float loadFactor) {
        super(1<<initialCapacityBitCount, loadFactor);
        bitMaps = new int[(int)(bitIndexMaxValue>>5)];
        this.indexForNum = threshold-1;
    }

    @Override
    public int get(long key) {
        if(key < bitIndexMaxValue) {
            if(bitScan((int)key) == 1) {
                Entry entry = getEntry(key);
                return null == entry ? -1 : entry.getValue();
            } else {
                return -1;
            }
        } else {
//            counter.incrementAndGet();
            Entry entry = getEntry(key);
            return null == entry ? -1 : entry.getValue();
        }
    }

    @Override
    void addEntry(int hash, long key, int value, int bucketIndex) {
        if ((size >= threshold) && (null != table[bucketIndex])) {
            resize(2 * table.length);
            hash = hash(key);
            bucketIndex = indexFor(hash, table.length);
        }
        if(key < bitIndexMaxValue) {
            bitUpdate((int) key, true);
        }
        createEntry(hash, key, value, bucketIndex);
    }

    @Override
    public int remove(long key) {
        if(key < bitIndexMaxValue) {
            bitUpdate((int) key, false);
        }
        Entry e = removeEntryForKey(key);
        if(e != null && removedEntry.size() < DEQUE_SIZE) {
            e.reset();
            removedEntry.push(e);
        }
        return (e == null ? -1 : e.value);
    }

    @Override
    int indexFor(int h, int length) {
        // assert Integer.bitCount(length) == 1 : "length must be a non-zero power of 2";
        return h & indexForNum;
    }

    private int bitScan(int key) {
        --key;
        int blockNumber = key >> 5;
        int curpos = key & 0x1F;
        int value = bitMaps[blockNumber];
        int bitValue = (value >> curpos) & 0x01;
        return bitValue;
    }

    public void bitUpdate(int key, boolean bitFlag) {
        --key;
        int blockNumber = key >> 5;
        int curpos = key & 0x1F;
        int value = bitMaps[blockNumber];
        if(bitFlag == true){
            value = value | (1 << curpos);
        }else{
            value = value ^ (1 << curpos);
        }

        bitMaps[blockNumber] = value;
    }
    public static void main(String[] args) {
        int a = 32 - Integer.numberOfLeadingZeros(1024);
        MyLong2IntWithBitIndexHashMap map = new MyLong2IntWithBitIndexHashMap(32-Integer.numberOfLeadingZeros(10000000/REDO_NUM), 0.9f);
        for(int i = 1; i < 10000000; i+=3) {
            map.put(i, i);
        }
        long begin = System.nanoTime();
        for(int j = 1; j < 10; j++) {
            for (int i = 1; i < 10000000; i++) {
                map.get(i);
            }
        }
        long end = System.nanoTime();
        System.out.println((end-begin));
    }
}
