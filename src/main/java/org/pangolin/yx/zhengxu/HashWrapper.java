package org.pangolin.yx.zhengxu;


import org.pangolin.xuzhe.positiveorder.MyLong2IntHashMap;

/**
 * Created by yangxiao on 2017/6/25.
 */
public class HashWrapper {

    MyLong2IntHashMap hashing = new MyLong2IntHashMap(1 << 19);

    public void put(long key, int value) {
        hashing.put(key, value);
    }

    public void remove(long key) {
        hashing.remove(key);
    }

    public int get(long key) {
        return hashing.get(key);
    }

    public int getOrDefault(long key, int dft) {
        if (hashing.containsKey(key)) {
            return hashing.get(key);
        } else {
            return dft;
        }
    }

    public boolean containsKey(long key) {
        return hashing.containsKey(key);
    }

}
