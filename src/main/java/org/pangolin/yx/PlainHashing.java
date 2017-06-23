package org.pangolin.yx;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by yangxiao on 2017/6/21.
 */
public class PlainHashing {

    private ArrayList<byte[]> bytes = new ArrayList<>();
    private final int BUFFER_SIZE = 1024 * 1024;
    private static int CHAIN_BLOCK_SIZE = 16;//链表节点的大小 4字节指向下一节点 8key 4value
    private int hashBits;
    private int[] hashTable;

    private int readInt(ArrayList<byte[]> buffer, int off) {
        int index = off / BUFFER_SIZE;
        int buffOff = off % BUFFER_SIZE;
        byte[] buf = buffer.get(index);
        int re = (buf[buffOff] & 0xff)
                | ((buf[buffOff + 1] & 0xff) << 8)
                | ((buf[buffOff + 2] & 0xff) << 16)
                | ((buf[buffOff + 3] & 0xff) << 24);
        return re;
    }

    private long readLong(ArrayList<byte[]> buffer, int off) {
        int index = off / BUFFER_SIZE;
        int buffOff = off % BUFFER_SIZE;
        byte[] buf = buffer.get(index);
        long re = (((long) buf[buffOff]) & 0xff)
                | (((long) buf[buffOff + 1] & 0xff) << 8)
                | (((long) buf[buffOff + 2] & 0xff) << 16)
                | (((long) buf[buffOff + 3] & 0xff) << 24)
                | (((long) buf[buffOff + 4] & 0xff) << 32)
                | (((long) buf[buffOff + 5] & 0xff) << 40)
                | (((long) buf[buffOff + 6] & 0xff) << 48)
                | (((long) buf[buffOff + 7] & 0xff) << 56);
        return re;
    }

    private void writeInt(ArrayList<byte[]> buffer, int off, int v) {
        int index = off / BUFFER_SIZE;
        int buffOff = off % BUFFER_SIZE;
        byte[] buf = buffer.get(index);
        buf[buffOff] = (byte) (0xff & v);
        buf[buffOff + 1] = (byte) (0xff & v >>> 8);
        buf[buffOff + 2] = (byte) (0xff & v >>> 16);
        buf[buffOff + 3] = (byte) (0xff & v >>> 24);
    }

    private void writeLong(ArrayList<byte[]> buffer, int off, long v) {
        int index = off / BUFFER_SIZE;
        int buffOff = off % BUFFER_SIZE;
        byte[] buf = buffer.get(index);
        buf[buffOff] = (byte) (0xff & v);
        buf[buffOff + 1] = (byte) (0xff & v >>> 8);
        buf[buffOff + 2] = (byte) (0xff & v >>> 16);
        buf[buffOff + 3] = (byte) (0xff & v >>> 24);

        buf[buffOff + 4] = (byte) (0xff & v >>> 32);
        buf[buffOff + 5] = (byte) (0xff & v >>> 40);
        buf[buffOff + 6] = (byte) (0xff & v >>> 48);
        buf[buffOff + 7] = (byte) (0xff & v >>> 56);
    }


    public PlainHashing(int capBit) {
        hashBits = capBit;
        hashTable = new int[1 << hashBits];
        bytes.add(new byte[BUFFER_SIZE]);
    }

    int allocatePos = CHAIN_BLOCK_SIZE;

    private int allocateChainNode() {
        if (allocatePos + CHAIN_BLOCK_SIZE > bytes.size() * BUFFER_SIZE) {
            bytes.add(new byte[BUFFER_SIZE]);
            allocatePos = (bytes.size() - 1) * BUFFER_SIZE;
        }
        int re = allocatePos;
        allocatePos += CHAIN_BLOCK_SIZE;
        return re;
    }

    public void put(long key, int value) {
        int pos = (int) (key & (hashTable.length - 1));
        int head = hashTable[pos];
        int oldHead = head;
        while (head != 0) {
            long k = readLong(bytes, head + 4);
            if (k == key) {
                writeInt(bytes, head + 12, value);
                return;
            }
            head = readInt(bytes, head);
        }

        int newNode = allocateChainNode();
        writeInt(bytes, newNode, oldHead);
        writeLong(bytes, newNode + 4, key);
        writeInt(bytes, newNode + 12, value);
        hashTable[pos] = newNode;
    }

    public void remove(long key) {
        int pos = (int) (key & (hashTable.length - 1));
        int head = hashTable[pos];
        if (head != 0) {
            long k = readLong(bytes, head + 4);
            int next = readInt(bytes, head);
            if (k == key) {
                hashTable[pos] = next;
                return;
            }
            int preNode = head;
            int curNode = next;
            while (curNode != 0) {
                long k1 = readLong(bytes, curNode + 4);
                int next1 = readInt(bytes, curNode);
                if (k1 == key) {
                    writeInt(bytes, preNode, next1);
                    return;
                }
                preNode = curNode;
                curNode = next1;
            }
        }
    }

    public int get(long key) throws Exception {
        int pos = (int) (key & (hashTable.length - 1));
        int head = hashTable[pos];
        while (head != 0) {
            long k = readLong(bytes, head + 4);
            if (k == key) {
                return readInt(bytes, head + 12);
            }
            head = readInt(bytes, head);
        }
        throw new Exception("no such key");
    }

    public int getOrDefault(long key, int dft) {
        int pos = (int) (key & (hashTable.length - 1));
        int head = hashTable[pos];
        while (head != 0) {
            long k = readLong(bytes, head + 4);
            if (k == key) {
                return readInt(bytes, head + 12);
            }
            head = readInt(bytes, head);
        }
        return dft;
    }

    public boolean containsKey(long key) {
        int pos = (int) (key & (hashTable.length - 1));
        int head = hashTable[pos];
        while (head != 0) {
            long k = readLong(bytes, head + 4);
            if (k == key) {
                return true;
            }
            head = readInt(bytes, head);
        }
        return false;
    }

    private static void run() throws Exception {
        PlainHashing lhash = new PlainHashing(21);
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
