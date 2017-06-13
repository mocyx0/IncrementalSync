package org.pangolin.xuzhe.stringparser;

import java.util.*;

/**
 * Created by ubuntu on 17-6-8.
 */
public class LocalLogIndex {
    Map<Long, List<IndexEntry>> indexes = new HashMap<>();
    private static MyLong2IntHashMap[] pkLastPosMap = new MyLong2IntHashMap[10];
    static {
//        HashLongIntMap map = makeMap(200_0000, 0.9f);
        for(int i = 0; i < 10; i++) {
            pkLastPosMap[i] = new MyLong2IntHashMap(1000_000, 0.9f);
//            System.out.println(SizeOf.humanReadable(SizeOf.deepSizeOf(pkLastPosMap[i])));
        }
    }

    public static final long[][] indexesArrays = new long[10][400_0000];
//    public static final AtomicInteger nextIndexPos = new AtomicInteger(0); // 使用getAndIncrement
    public static int[] nextIndexPos = new int[10]; // 使用getAndIncrement
    public LocalLogIndex() {

    }

    public static long[] getAllIndexesByPK(long pk) {
        long[] tmp = new long[1024];
        int idx = 0; // 结果数组中存当前数据的索引
        for(int i = 0; i < 10; i++) {
            int lastPos = pkLastPosMap[i].get(pk);
            if (lastPos == 0) {
                continue;
            }
            int pos = lastPos;
            if(pos == 0xFFFFFF) {
                pos = 0;
            }
            long index = indexesArrays[i][pos]; // 记录的当前一条索引信息
            tmp[idx] = index;
            ++idx;
            while ((pos = getPrevIndexFromLong(index)) != 0xFFFFFF) {
                index = indexesArrays[i][pos]; // 记录的当前一条索引信息
                tmp[idx] = index;
                ++idx;
            }
        }
        long[] result = Arrays.copyOf(tmp, idx);
        Arrays.sort(result);
        for(int i = 0; i < result.length/2; i++) {
            long _tmp = result[i];
            result[i] = result[result.length-i-1];
            result[result.length-i-1] = _tmp;
        }
        return result;
    }

    public static int getFileNoFromLong(long index) {
        index = (index >> 56) & 0xFF;
        return (int)index;
    }

    public static int getPositionFromLong(long index) {
        index = (index >> 24) & 0xFFFFFFFF;
        return (int)index;
    }

    public static int getPrevIndexFromLong(long index) {
        index = index & 0xFFFFFF;
        if(index == 0xFFFFFF)
            return 0;
        if(index == 0)
            return 0xFFFFFF;
        return (int)index;
    }

    private static long makeIndex(int fileNo, int position) {
        long result = fileNo;
        result = result << 56;
        result = (result + (((long)position)<<24));
        return result;
    }

    // 先按照只有1个文件来写
    public synchronized static void appendIndex2(long pk, int fileNo, int position) {
        int lastPos = pkLastPosMap[fileNo-1].get(pk);
        int pos;
//        if(lastPos == 0) { // 没有数据
//            pos = 0;  // 因LongIntMap的默认值为0，因此0用0xFFFFFF代替
        pos = lastPos;

//        int currentIndexPos = nextIndexPos.getAndIncrement();
        int currentIndexPos = nextIndexPos[fileNo-1]++;

        if(currentIndexPos == 0) {
            pkLastPosMap[fileNo-1].put(pk, 0xFFFFFF);

        } else {
            pkLastPosMap[fileNo-1].put(pk, currentIndexPos);
        }
        long index = makeIndex(fileNo, position);
        pos = pos & 0xFFFFFF;
        index = index | pos;
        indexesArrays[fileNo-1][currentIndexPos] = index;
    }

    public void appendIndex(long pk, int fileNo, int position) {
        List<IndexEntry> list = indexes.get(pk);
        synchronized (this){
            if(list == null) {
                list = new ArrayList<>();
                Collections.synchronizedList(list);
                indexes.put(pk, list);
            }
        }
        list.add(new IndexEntry(fileNo, position));
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Size:");
        builder.append(indexes.size());
//        builder.append("\n");
//        for(Map.Entry<Long, List<IndexEntry>> entry : indexes.entrySet()) {
//            builder.append(entry.getKey());
//            builder.append("\n");
//            for(IndexEntry item : entry.getValue()) {
//                builder.append(item);
//                builder.append("\n");
//            }
//        }
        return builder.toString();
    }

    public static LocalLogIndex merge(LocalLogIndex[] indexes) {
        LocalLogIndex result = new LocalLogIndex();
        for(LocalLogIndex index : indexes) {
            for(Map.Entry<Long, List<IndexEntry>> entry : index.indexes.entrySet()) {
                List<IndexEntry> list = result.indexes.get(entry.getKey());
                if(list == null) {
                    list = new ArrayList<>();
                    result.indexes.put(entry.getKey(), list);
                }
                list.addAll(entry.getValue());
            }
        }
        return result;
    }



    public static class IndexEntry implements Comparable<IndexEntry> {
        public final int fileNo;
        public final int position;

        private IndexEntry(int fileNo, int position) {
            this.fileNo = fileNo;
            this.position = position;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            sb.append(fileNo);
            sb.append(',');
            sb.append(position);
            sb.append(')');
            return sb.toString();
        }

        @Override
        public int compareTo(IndexEntry indexEntry) {
            if(this.fileNo != indexEntry.fileNo) return this.fileNo - indexEntry.fileNo;
            return (this.position - indexEntry.position);
        }
    }

}
