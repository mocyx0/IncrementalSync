package org.pangolin.xuzhe.stringparser;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.pangolin.xuzhe.stringparser.ReadingThread.getLineByPosition;

/**
 * Created by ubuntu on 17-6-8.
 */
public class Redo {

    public static Record redo(LocalLogIndex indexes, long pk) {
        List<LocalLogIndex.IndexEntry> logs = getIndexEntries(indexes, pk);  //获取主键对应list
        Record r;
        LocalLogIndex.IndexEntry index = logs.get(0);
        String line = getLineByPosition(index.fileNo, index.position);
        Log log = Log.parser(line);
        r = Record.createFromLastLog(log, indexes, index.fileNo, index.position);  //对最后一条log创建record
        if (log.op == 'U') {
            updateResult(r, log, index);                        //无论该log的类型是什么都应该先进行result的更新
            updateCurrentIdKey(log);
            log = judgeUpdate(r, log, indexes);
        }
        if (log.op == 'I') {
            updateResult(r, log, index);
        }
        return r;
    }

    private static Log judgeUpdate(Record r, Log log, LocalLogIndex indexes) {
        List<LocalLogIndex.IndexEntry> logs;
        while (log.op != 'I') {
            logs = getIndexEntries(indexes, log.getCurrentOldKey());
            log = update(r, logs);
        }
        return log;
    }
    private static Log update(Record r, List<LocalLogIndex.IndexEntry> logs) {
        Log log = null;
        for (LocalLogIndex.IndexEntry index : logs) {
            if (index.fileNo <= r.getFileNo() && index.position < r.getPosition()) {
                String line = getLineByPosition(index.fileNo, index.position);
                log = Log.parser(line);
                if (log.op == 'U') {
                    updateResult(r, log, index);
                    updateCurrentIdKey(log);
                    if (log.getCurrentOldKey() != log.getCurrentNewkey()) {
                        break;
                    }
                }
            }
        }
        return log;
    }

    private static void updateCurrentIdKey(Log log) {
        for (ColumnLog columnLog : log.columns) {
            if (columnLog.columnInfo.isPK) {
                log.setCurrentOldKey(columnLog.oldLongValue);
                log.setCurrentNewkey(columnLog.newLongValue);
                break;
            }
        }
    }

    private static void updateResult(Record r, Log log, LocalLogIndex.IndexEntry index) {
        r.updateResult(log);
        r.setFileNo(index.fileNo);
        r.setPosition(index.position);
    }


    private static List<LocalLogIndex.IndexEntry> getIndexEntries(LocalLogIndex indexes, long pk) {
        List<LocalLogIndex.IndexEntry> logs = indexes.indexes.get(pk);
        if (!Record.getList().contains(pk)) {   //包含pk说明pk的List已经排过序了
            Collections.sort(logs);
            Collections.reverse(logs);
            Record.getList().add(pk);
        }
        return logs;
    }
//        List<LocalLogIndex.IndexEntry> logs = indexes.indexes.get(pk);
//        if(!Record.getList().contains(pk)){   //包含pk说明pk的List已经排过序了
//            Collections.sort(logs);
//            Collections.reverse(logs);
//            Record.getList().add(pk);
//        }
//        Record r = null;
//        int count = 0;
//        for(LocalLogIndex.IndexEntry index : logs) {
//            String line = getLineByPosition(index.fileNo, index.position);
//            Log log = Log.parser(line);
//            if(log.op == 'D'){
//                break;
//            }
//            if(count == 0) {
//                r = Record.createFromLastLog(log, indexes, index.fileNo, index.position);
//            }else{
//                r.update(log, indexes, index.fileNo, index.position);
//            }
//            if(log.op == 'I'){
//                break;
//            }
//            ++count;
//        }
//        return r;


}
