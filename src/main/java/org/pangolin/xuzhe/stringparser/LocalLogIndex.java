package org.pangolin.xuzhe.stringparser;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ubuntu on 17-6-8.
 */
public class LocalLogIndex {
    Map<Long, List<IndexEntry>> indexes = new HashMap<>();
    private static Map<Long, Integer> pkLastPosMap = new HashMap<>();
    public static final long[] indexesArray_1 = new long[1500_0000];  // 保存1号文件中的索引
    public static final AtomicInteger nextIndexPos = new AtomicInteger(0); // 使用getAndIncrement
    public LocalLogIndex() {

    }

    public long[] getAllIndexesByPK(long pk) {
        return null;
    }

    private static long makeIndex(int fileNo, int position) {
        long result = fileNo;
        result = result << 56;
        result = (result + ((long)position)<<24);
        return result;
    }

    // 先按照只有1个文件来写
    public static void appendIndex2(long pk, int fileNo, int position) {
        Integer lastPos = pkLastPosMap.get(pk);
        int pos;
        if(lastPos == null) {
            pos = -1;
        } else {
            pos = lastPos;
        }
        int currentIndexPos = nextIndexPos.getAndIncrement();

        pkLastPosMap.put(pk, currentIndexPos);
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
