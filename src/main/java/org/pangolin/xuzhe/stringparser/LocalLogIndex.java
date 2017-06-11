package org.pangolin.xuzhe.stringparser;

import java.util.*;

/**
 * Created by ubuntu on 17-6-8.
 */
public class LocalLogIndex {
    Map<Long, List<IndexEntry>> indexes = new HashMap<>();

    public LocalLogIndex() {

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

    public static class matchingEntry{
        private final long id;
        private final char type;
        private final long preId;
        private final long nextId;

        public matchingEntry(long id, char type, long preId, long nextId) {
            this.id = id;
            this.type = type;
            this.preId = preId;
            this.nextId = nextId;
        }

        public long getId() {
            return id;
        }

        public char getType() {
            return type;
        }

        public long getPreId() {
            return preId;
        }

        public long getNextId() {
            return nextId;
        }
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
