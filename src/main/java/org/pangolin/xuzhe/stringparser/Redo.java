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
        Record r = null;
        int count = 0;
        for(LocalLogIndex.IndexEntry index : logs) {
            String line = getLineByPosition(index.fileNo, index.position);
            Log log = Log.parser(line);
            if(log.op == 'D'){
                break;
            }
            if(count == 0) {
                r = Record.createFromLastLog(log, indexes);
            }else {
                r.update(Log.parser(line), indexes);
            }
            if(log.op == 'I'){
                break;
            }
            ++count;
        }
        return r;
    }
}
