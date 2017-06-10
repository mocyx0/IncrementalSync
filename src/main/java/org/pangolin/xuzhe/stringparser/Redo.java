package org.pangolin.xuzhe.stringparser;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.pangolin.xuzhe.stringparser.ReadingThread.getLineByPosition;

/**
 * Created by ubuntu on 17-6-8.
 */
public class Redo {
//    public static Record redo(LocalLogIndex indexes, long pk) {
//        List<LocalLogIndex.IndexEntry> logs = indexes.indexes.get(pk);
//        Collections.sort(logs);
//        Collections.reverse(logs);
//        Record r = null;
//        int count = 0;
//        for(LocalLogIndex.IndexEntry index : logs) {
//            String line = getLineByPosition(index.fileNo, index.position);
//            Log log = Log.parser(line);
//            if(log.op == 'D'){
//                break;
//            }
//            if(count == 0) {
//                r = Record.createFromLastLog(log, indexes);
//            }else {
//                r.update(Log.parser(line), indexes);
//            }
//            if(log.op == 'I'){
//                break;
//            }
//            ++count;
//        }
//        return r;
//    }

    public static Record redo(LocalLogIndex indexes, long pk) {
        List<LocalLogIndex.IndexEntry> logs = indexes.indexes.get(pk);
        if(!Record.getList().contains(pk)){   //包含pk说明pk的List已经排过序了
            Collections.sort(logs);
            Collections.reverse(logs);
            Record.getList().add(pk);
        }
        Record r = null;
        int count = 0;
        LocalLogIndex.IndexEntry index = logs.get(0);
        String line = getLineByPosition(index.fileNo, index.position);
        Log log = Log.parser(line);
        r = Record.createFromLastLog(log, indexes, index.fileNo, index.position);
        if(log.op == 'U'){
            r.insertResult(log);
                long oldKey = 0, newKey = 0;
                for (ColumnLog columnLog : log.columns) {
                    if (columnLog.columnInfo.isPK) {
                        oldKey = columnLog.oldLongValue;
                        newKey = columnLog.newLongValue;
                        break;
                    }
                }
                log = judgeUpdate(r,log, indexes,oldKey,newKey);
            }
            if(log.op == 'I'){
                r.insertResult(log);
            }
        return r;
    }
    private static Log judgeUpdate(Record r, Log log, LocalLogIndex indexes , long oldPk, long newPk){
       while( log.op != 'I'){
           List<LocalLogIndex.IndexEntry> logs = indexes.indexes.get(oldPk);
           if(!Record.getList().contains(oldPk)) {   //包含pk说明pk的List已经排过序了
               Collections.sort(logs);
               Collections.reverse(logs);
               Record.getList().add(oldPk);
           }
           for(LocalLogIndex.IndexEntry index : logs) {
               if(index.fileNo <= r.getFileNo() && index.position < r.getPosition()){
                   String line = getLineByPosition(index.fileNo, index.position);
                   r.setFileNo(index.fileNo);
                   r.setPosition(index.position);
                   log = Log.parser(line);
                   if(log.op == 'U'){
                       r.insertResult(log);
                       for (ColumnLog columnLog : log.columns) {
                           if (columnLog.columnInfo.isPK) {
                               oldPk = columnLog.oldLongValue;
                               newPk = columnLog.newLongValue;
                               break;
                           }
                       }
                       if(oldPk != newPk){
                           break;
                       }
                   }
               }
           }
       }
    return  log;
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
