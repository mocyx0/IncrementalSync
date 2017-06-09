package org.pangolin.xuzhe.stringparser;

import java.util.Collections;
import java.util.List;

import static org.pangolin.xuzhe.stringparser.ReadingThread.getLineByPosition;

/**
 * Created by ubuntu on 17-6-8.
 */
public class Redo {
    public static Record redo(LocalLogIndex indexes, long pk) {
        List<LocalLogIndex.IndexEntry> logs = indexes.indexes.get(pk);
        Collections.sort(logs);
        Collections.reverse(logs);
        for(LocalLogIndex.IndexEntry index : logs) {
            String line = getLineByPosition(index.fileNo, index.position);
            Record r = Record.createFromLastLog(Log.parser(line));
        }
        return null;
    }
}
