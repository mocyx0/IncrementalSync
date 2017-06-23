package org.pangolin.yx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by yangxiao on 2017/6/23.
 */
public class PlainHashArr {
    private static class DataArr {
        long[] keys;
        int[] values;
        int[] nexts;
    }

    private static final int BUFFER_SIZE = 1024 * 16;
    private static final int BUFFER_SIZE_BIT = 14;
    private int hashBits;
    private int[] hashTable;
    private DataArr dataArr = new DataArr();
    private LinkedList<Integer> freeNode = new LinkedList<>();


    public PlainHashArr(int capBit) {
        hashBits = capBit;
        hashTable = new int[1 << hashBits];
        dataArr.keys = new long[1 << (hashBits + 1)];
        dataArr.values = new int[1 << (hashBits + 1)];
        dataArr.nexts = new int[1 << (hashBits + 1)];
    }

    int allocatePos = 0;

    private int allocateChainNode() {
        if (freeNode.size() == 0) {
            allocatePos++;
            return allocatePos;
        } else {
            return freeNode.poll();
        }
    }


    public void put(long key, int value) {
        int pos = (int) (key & (hashTable.length - 1));
        int head = hashTable[pos];
        int oldHead = head;
        while (head != 0) {
            if (dataArr.keys[head] == key) {
                dataArr.values[head] = value;
                return;
            }
            head = dataArr.nexts[head];
        }
        int newNode = allocateChainNode();
        dataArr.nexts[newNode] = oldHead;
        dataArr.keys[newNode] = key;
        dataArr.values[newNode] = value;
        hashTable[pos] = newNode;
    }

    public void remove(long key) {
        int pos = (int) (key & (hashTable.length - 1));
        int head = hashTable[pos];
        if (head != 0) {
            if (dataArr.keys[head] == key) {
                freeNode.add(head);
                hashTable[pos] = dataArr.nexts[head];
                return;
            }
            int preNode = head;
            int curNode = dataArr.nexts[head];
            while (curNode != 0) {
                if (dataArr.keys[curNode] == key) {
                    freeNode.add(curNode);
                    dataArr.nexts[preNode] = dataArr.nexts[curNode];
                    return;
                }
                preNode = curNode;
                curNode = dataArr.nexts[curNode];
            }
        }
    }

    public int get(long key) throws Exception {
        int pos = (int) (key & (hashTable.length - 1));
        int head = hashTable[pos];
        while (head != 0) {
            if (dataArr.keys[head] == key) {
                return dataArr.values[head];
            }
            head = dataArr.nexts[head];
        }
        throw new Exception("no such key");
    }

    public int getOrDefault(long key, int dft) {
        int pos = (int) (key & (hashTable.length - 1));
        int head = hashTable[pos];
        while (head != 0) {
            if (dataArr.keys[head] == key) {
                return dataArr.values[head];
            }
            head = dataArr.nexts[head];
        }
        return dft;
    }

    public boolean containsKey(long key) {
        int pos = (int) (key & (hashTable.length - 1));
        int head = hashTable[pos];
        while (head != 0) {
            if (dataArr.keys[head] == key) {
                return true;
            }
            head = dataArr.nexts[head];
        }
        return false;
    }

    private static void run() throws Exception {
        PlainHashArr lhash = new PlainHashArr(21);
        HashMap<Long, Integer> hashMap = new HashMap<>();
        //测试数据量
        int testCount = 1500000;
        long seq = 0;

        long t1 = System.currentTimeMillis();
        System.out.println("hashmao start");
        //hashmap
        for (int i = 0; i < testCount; i++) {
            hashMap.put((long) i, i);
        }
        for (int i = 0; i < testCount / 2; i++) {
            hashMap.remove((long) i * 2);
        }
        for (int i = 0; i < testCount; i++) {
            long key = i;
            if (hashMap.containsKey(key)) {
                seq++;
            }
        }
        long t2 = System.currentTimeMillis();
        System.out.println("lhash start");
        //lhash
        for (int i = 0; i < testCount; i++) {
            lhash.put((long) i, i);
        }
        for (int i = 0; i < testCount / 2; i++) {
            lhash.remove(i * 2);
        }
        for (int i = 0; i < testCount; i++) {
            long key = i;
            if (lhash.containsKey(key)) {
                seq++;
            }
        }
        long t3 = System.currentTimeMillis();
        for (int i = 0; i < testCount; i++) {
            boolean b1 = hashMap.containsKey((long) i);
            boolean b2 = lhash.containsKey(i);
            if (b1 && b2) {
                int v1 = hashMap.get((long) i);
                int v2 = lhash.get(i);
                if (v1 != v2) {
                    System.out.println("error");
                }
            } else if (!b1 && !b2) {
            } else {
                System.out.println("error");
            }
        }

        System.out.println(String.format("hashmap %d lhash %d lhashSize %d", t2 - t1, t3 - t2, LinearHashing.TOTAL_MEM.get()));
    }

    public static void main(String[] args) throws Exception {
        run();
        Thread.sleep(100000);
    }
}
