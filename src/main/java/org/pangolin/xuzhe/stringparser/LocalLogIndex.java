package org.pangolin.xuzhe.stringparser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ubuntu on 17-6-8.
 */
public class LocalLogIndex {
    Map<Long, List<IndexEntry>> indexes = new HashMap<>();

    public LocalLogIndex() {

    }

    public void appendIndex(long pk, long timestamp, int fileNo, int position) {
        List<IndexEntry> list = indexes.get(pk);
        if(list == null) {
            list = new ArrayList<>();
            indexes.put(pk, list);
        }
        list.add(new IndexEntry(timestamp, fileNo, position));
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Size:");
        builder.append(indexes.size());
        builder.append("\n");
        for(Map.Entry<Long, List<IndexEntry>> entry : indexes.entrySet()) {
            builder.append(entry.getKey());
            builder.append("\n");
            for(IndexEntry item : entry.getValue()) {
                builder.append(item);
                builder.append("\n");
            }
        }
        return builder.toString();
    }

    private static class IndexEntry {
        public final long timestamp;
        public final int fileNo;
        public final int position;

        public IndexEntry(long timestamp, int fileNo, int position) {
            this.timestamp = timestamp;
            this.fileNo = fileNo;
            this.position = position;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            sb.append(timestamp);
            sb.append(',');
            sb.append(fileNo);
            sb.append(',');
            sb.append(position);
            sb.append(')');
            return sb.toString();
        }
    }
}
