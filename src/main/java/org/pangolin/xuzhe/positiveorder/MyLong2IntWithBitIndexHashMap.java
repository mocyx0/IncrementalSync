package org.pangolin.xuzhe.positiveorder;

/**
 * Created by XuZhe on 2017/6/21.
 */
public class MyLong2IntWithBitIndexHashMap extends MyLong2IntHashMap {
    private int[] bitMaps;
    static private final long bitIndexMaxValue = 1<<24; // 16777216
    public MyLong2IntWithBitIndexHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
        bitMaps = new int[(int)(bitIndexMaxValue>>5)];
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

    private int bitScan(int key){
        --key;
        int blockNumber = key >> 5;
        int curpos = key & 0x1F;
        int value = bitMaps[blockNumber];
        int bitValue = (value >> curpos) & 0x01;
        return bitValue;
    }

    public void bitUpdate(int key, boolean bitFlag){
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
}
