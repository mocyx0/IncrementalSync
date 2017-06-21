package org.pangolin.yx;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by yangxiao on 2017/6/21.
 */
public class PlainHashingSimple {
    private static class Node {
        Node next;
        int v;
        long k;
    }

    private LinkedList<Node> freeNode = new LinkedList<>();
    private Node[] hashTable;
    private int hashBits;

    public PlainHashingSimple(int capBit) {
        hashBits = capBit;
        hashTable = new Node[1 << hashBits];
    }

    Node allocateChainNode() {
        if (freeNode.size() == 0) {
            return new Node();
        } else {
            return freeNode.poll();
        }
    }


    public void put(long key, int value) {
        int pos = (int) (key & (hashTable.length - 1));
        Node head = hashTable[pos];
        Node oldHead = head;
        while (head != null) {
            if (head.k == key) {
                head.v = value;
                return;
            }
            head = head.next;
        }
        Node newNode = allocateChainNode();
        newNode.k = key;
        newNode.v = value;
        newNode.next = oldHead;
        hashTable[pos] = newNode;
    }

    public void remove(long key) {
        int pos = (int) (key & (hashTable.length - 1));
        Node head = hashTable[pos];
        if (head != null) {
            if (head.k == key) {
                hashTable[pos] = head.next;
                freeNode.add(head);
                return;
            }
            Node preNode = head;
            Node curNode = head.next;
            while (curNode != null) {
                if (curNode.k == key) {
                    //writeInt(bytes, preNode, next1);
                    preNode.next = curNode.next;
                    freeNode.add(curNode);
                    return;
                }
                preNode = curNode;
                curNode = curNode.next;
            }
        }
    }

    public int get(long key) throws Exception {
        int pos = (int) (key & (hashTable.length - 1));
        Node head = hashTable[pos];
        while (head != null) {
            if (head.k == key) {
                return head.v;
            }
            head = head.next;
        }
        throw new Exception("no such key");
    }

    public int getOrDefault(long key, int dft) {
        int pos = (int) (key & (hashTable.length - 1));
        Node head = hashTable[pos];
        while (head != null) {
            if (head.k == key) {
                return head.v;
            }
            head = head.next;
        }
        return dft;
    }

    public boolean containsKey(long key) {
        int pos = (int) (key & (hashTable.length - 1));
        Node head = hashTable[pos];
        while (head != null) {
            if (head.k == key) {
                return true;
            }
            head = head.next;
        }
        return false;
    }

    private static void run() throws Exception {
        PlainHashingSimple lhash = new PlainHashingSimple(21);
        HashMap<Long, Integer> hashMap = new HashMap<>();
        //测试数据量
        int testCount = 2000000;
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
