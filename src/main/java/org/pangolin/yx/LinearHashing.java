package org.pangolin.yx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/6/8.
 * <p>
 * 注意我们不需要实现remove,size 因为我们不用到这个操作
 */
public class LinearHashing {
    public static AtomicInteger TOTAL_MEM = new AtomicInteger();

    private static int BUFFER_SIZE = 1024 * 1024;//
    private static int BLOCK_SIZE = 4;//8字节的key 4字节value,
    private static int CHAIN_BLOCK_SIZE = 16;//链表节点的大小 4字节指向下一节点 8key 4value
    //private static int SLOT_COUNT = 1;//单个block包含的节点数量
    private static float LOAD_FACTOR = 0.75f;

    private ArrayList<byte[]> blockBuffer = new ArrayList<>();
    //字节格式: int 指向下一个node | long key |int value
    private ArrayList<byte[]> chainBuffer = new ArrayList<>();

    int freeChain = 0;
    int deleteChain = 0;

    private int n;//block count
    private int i;//number of bits used in hash
    private int r;//data count

    public int size() {
        return r;
    }

    public LinearHashing() {
        n = 2;
        i = 1;
        r = 0;
        freeChain = CHAIN_BLOCK_SIZE;
        blockBuffer.add(new byte[BUFFER_SIZE]);
        chainBuffer.add(new byte[BUFFER_SIZE]);
        updateMemCount();
        updateMemCount();

    }

    private void updateMemCount() {
        //TOTAL_MEM.addAndGet(BUFFER_SIZE);
    }


    private int hashCode(long key) {
        int hi = (int) (key >>> 32);
        int lo = (int) (key);
        return hi ^ lo;
    }

    private int getBlock(int hash) {
        int m = hash & ((1 << i) - 1);
        if (m < n) {
            return m * BLOCK_SIZE;
        } else {
            return (m ^ (1 << (i - 1))) * BLOCK_SIZE;
        }
    }

    private void freeChainNode(int off) {
        writeInt(chainBuffer, deleteChain, off);
        deleteChain = off;
    }

    private int allocateChainNode() {
        if (deleteChain != 0) {
            int re = deleteChain;
            deleteChain = readInt(chainBuffer, deleteChain);
            return re;
        } else {
            if (freeChain + CHAIN_BLOCK_SIZE > chainBuffer.size() * BUFFER_SIZE) {
                chainBuffer.add(new byte[BUFFER_SIZE]);
                updateMemCount();
                freeChain = (chainBuffer.size() - 1) * BUFFER_SIZE;
            }
            int re = freeChain;
            freeChain += CHAIN_BLOCK_SIZE;
            return re;
        }
    }

    private int readInt(byte[] buf, int buffOff) {
        int re = (buf[buffOff] & 0xff)
                | ((buf[buffOff + 1] & 0xff) << 8)
                | ((buf[buffOff + 2] & 0xff) << 16)
                | ((buf[buffOff + 3] & 0xff) << 24);
        return re;
    }

    private long readLong(byte[] buf, int buffOff) {
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

    //如果存在key则更新 否则插入新值
    private void putToBuffer(int block, long key, int value) {

        //search first
        int blockHead = readInt(blockBuffer, block);
        //int c = 0;
        while (blockHead != 0) {
            //   c++;
            int next = readInt(chainBuffer, blockHead);
            long k = readLong(chainBuffer, blockHead + 4);
            if (k == key) {
                //System.out.println(c);
                writeInt(chainBuffer, blockHead + 12, value);
                return;
            }
            blockHead = next;
        }
        //System.out.println(c);
        //没有找到key 插入新值
        r++;
        //链表内存地址
        int newHead = allocateChainNode();
        /*
        int head = readInt(blockBuffer, block);
        //更新链表头指针
        writeInt(blockBuffer, block, newHead);
        //写链表节点
        writeInt(chainBuffer, newHead, head);
        */
        addToBlock(block, newHead);

        writeLong(chainBuffer, newHead + 4, key);
        writeInt(chainBuffer, newHead + 12, value);
    }

    private void newBlock() {
        if (n * BLOCK_SIZE > blockBuffer.size() * BUFFER_SIZE) {
            blockBuffer.add(new byte[BUFFER_SIZE]);
            updateMemCount();
        }
    }

    private void addToBlock(int block, int newHead) {
        /*
        if (block / BUFFER_SIZE >= blockBuffer.size()) {
            blockBuffer.add(new byte[BUFFER_SIZE]);
        }
        */
        int head = readInt(blockBuffer, block);
        writeInt(chainBuffer, newHead, head);
        writeInt(blockBuffer, block, newHead);
    }

    private void split() {
        int oldBlock = n % (1 << (i - 1)) * BLOCK_SIZE;
        n++;
        newBlock();
        if (n > (1 << i)) {
            i++;
        }
        //遍历bucketIndex
        int head = readInt(blockBuffer, oldBlock);
        //set empty
        writeInt(blockBuffer, oldBlock, 0);
        while (head != 0) {
            int next = readInt(chainBuffer, head);
            long k = readLong(chainBuffer, head + 4);
            int hash = hashCode(k);
            int block = getBlock(hash);
            //添加到block队列
            addToBlock(block, head);
            head = next;
        }
    }

    //注意删除操作不会回收内存
    public void remove(long key) {
        int hash = hashCode(key);
        int block = getBlock(hash);
        int blockHead = readInt(blockBuffer, block);
        if (blockHead != 0) {
            int headNext = readInt(chainBuffer, blockHead);
            long headK = readLong(chainBuffer, blockHead + 4);
            if (headK == key) {
                writeInt(blockBuffer, block, headNext);
                r--;
                return;
            }

            int preNode = blockHead;
            int curNode = readInt(chainBuffer, preNode);
            while (curNode != 0) {
                int next = readInt(chainBuffer, curNode);
                long k = readLong(chainBuffer, curNode + 4);
                if (k == key) {
                    r--;
                    writeInt(chainBuffer, preNode, next);
                    return;
                }
                preNode = curNode;
                curNode = next;
            }
        }

    }

    public void put(long k, int v) {
        int hash = hashCode(k);
        int block = getBlock(hash);
        putToBuffer(block, k, v);
        if (r / (float) n > LOAD_FACTOR) {
            split();
        }
        /*
        if (r / (float) n > LOAD_FACTOR) {
            split();
        }
        */
    }

    public int get(long k) throws Exception {
        int hash = hashCode(k);
        int block = getBlock(hash);
        int head = readInt(blockBuffer, block);
        while (head != 0) {
            int next = readInt(chainBuffer, head);
            long key = readLong(chainBuffer, head + 4);
            int value = readInt(chainBuffer, head + 12);
            if (key == k) {
                return value;
            }
            head = next;
        }
        throw new Exception("no such key");
    }

    public int getOrDefault(long k, int v) throws Exception {
        int hash = hashCode(k);
        int block = getBlock(hash);
        int head = readInt(blockBuffer, block);
        while (head != 0) {
            int index = head / BUFFER_SIZE;
            int buffOff = head % BUFFER_SIZE;
            byte[] buf = chainBuffer.get(index);
            int next = readInt(buf, buffOff);
            long key = readLong(buf, buffOff + 4);
            int value = readInt(buf, buffOff + 12);
            /*
            int next = readInt(chainBuffer, head);
            long key = readLong(chainBuffer, head + 4);
            int value = readInt(chainBuffer, head + 12);
            */
            if (key == k) {
                return value;
            }
            head = next;
        }
        return v;
    }

    public boolean containsKey(long k) {
        int hash = hashCode(k);
        int block = getBlock(hash);
        int head = readInt(blockBuffer, block);
        while (head != 0) {
            int next = readInt(chainBuffer, head);
            long key = readLong(chainBuffer, head + 4);
            if (key == k) {
                return true;
            }
            head = next;
        }
        return false;
    }

    private static void run() throws Exception {
        LinearHashing lhash = new LinearHashing();
        HashMap<Long, Integer> hashMap = new HashMap<>();
        //测试数据量
        int testCount = 7000000;
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
