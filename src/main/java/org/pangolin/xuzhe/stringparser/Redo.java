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
//        //     Integer logs = getIndexEntries(indexes, pk);  //获取主键对应list
//        Record r;
//        LocalLogIndex.IndexEntry index = logs.get(0);
//
//        //假设已经获取到pk对应的Integer叫idPos
//        Integer idPos = null;
//        byte fileNoIndex = (byte) ((idPos >> 24) & ((int) Math.pow(2, 8) - 1));
//        int lastPosition = idPos & ((int) Math.pow(2, 24) - 1);
//        //根据fileNo获取到long[] 名字叫做：logs,并且得到lastPosition对应的long型值
//        long[] logs = null;
//        long pos = logs[lastPosition];
//
//        byte fileNo = (byte) ((pos >> 56) & ((long) Math.pow(2, 8) - 1));
//        int position = (int) ((pos >> 24) & ((long) Math.pow(2, 32) - 1));
//        int preIndexPos = (int) (pos & ((long) Math.pow(2, 24) - 1));
//        String line = getLineByPosition(fileNo, position);
//        Log log = Log.parser(line);
//        r = Record.createFromLastLog(log, fileNo, position);  //对最后一条log创建record
//        if (log.op == 'U') {
//            updateResult(r, log, fileNo, position);                        //无论该log的类型是什么都应该先进行result的更新
//            updateCurrentIdKey(log);
//            log = judgeUpdate(r, log, indexes, preIndexPos);
//        }
//        if (log.op == 'I') {
//            updateResult(r, log, fileNo, position);
//        }
//        return r;
//    }
//    private static Log judgeUpdate(Record r, Log log, LocalLogIndex indexes , int preIndexPos) {
//        Integer logs;
//        while (log.op != 'I') {
//            logs = getIndexEntries(indexes, log.getCurrentOldKey());
//            log = update(r, logs);
//        }
//        return log;
//    }
//    private static Log update(Record r, Integer idPos) {
//        Log log = null;
//        //假设已经获取到pk对应的Integer叫idPos
//        byte fileNoIndex = (byte) ((idPos >> 24) & ((int) Math.pow(2, 8) - 1));
//        int lastPosition = idPos & ((int) Math.pow(2, 24) - 1);
//
//        long[] logs = null;
//        long pos = logs[lastPosition];
//
//        byte fileNo = (byte) ((pos >> 56) & ((long) Math.pow(2, 8) - 1));
//        int position = (int) ((pos >> 24) & ((long) Math.pow(2, 32) - 1));
//        int preIndexPos = (int) (pos & ((long) Math.pow(2, 24) - 1));
//
//        while(fileNo > r.getFileNo() || position > r.getPosition()){
//            pos = logs[preIndexPos];
//            fileNo = (byte) ((pos >> 56) & ((long) Math.pow(2, 8) - 1));
//            position = (int) ((pos >> 24) & ((long) Math.pow(2, 32) - 1));
//            preIndexPos = (int) (pos & ((long) Math.pow(2, 24) - 1));
//        }
//
//        while (fileNo <= r.getFileNo() && position < r.getPosition()) {
//            String line = getLineByPosition(fileNo, position);
//            log = Log.parser(line);
//            if (log.op == 'U') {
//                updateResult(r, log, fileNo, position);
//                updateCurrentIdKey(log);
//                if (log.getCurrentOldKey() != log.getCurrentNewkey()) {
//                    break;
//                }
//                pos = logs[preIndexPos];
//                fileNo = (byte) ((pos >> 56) & ((long) Math.pow(2, 8) - 1));
//                position = (int) ((pos >> 24) & ((long) Math.pow(2, 32) - 1));
//                preIndexPos = (int) (pos & ((long) Math.pow(2, 24) - 1));
//            }
//            if(log.op == 'I')
//                break;
//        }
//
//        return log;
//    }
//    private static void updateResult(Record r, Log log, byte fileNo, int position) {
//        r.updateResult(log);
//        r.setFileNo(fileNo);
//        r.setPosition(position);
//    }
//    private static Integer getIndexEntries(LocalLogIndex indexes, long pk) {
//        Integer logs = indexes.indexes.get(pk);
//        return logs;
//    }
//

    public static String[] redo(long pk) {

 //       List<LocalLogIndex.IndexEntry> logs = getIndexEntries(indexes, pk);  //获取主键对应list
        long[] _indexes = LocalLogIndex.getAllIndexesByPK(pk);
        Record r;
        String[] result = null;
      //  LocalLogIndex.IndexEntry index = logs.get(0);
        long index = _indexes[0];
        String line = getLineByPosition(LocalLogIndex.getFileNoFromLong(index), LocalLogIndex.getPositionFromLong(index));
        Log log = Log.parser(line);
        // TODO: 删除操作没有做处理
        r = Record.createFromLastLog(log, index);  //对最后一条log创建record
        if (log.op == 'U') {
            if(log.columns[0].newLongValue == pk){
                updateResult(r, log, index);                        //无论该log的类型是什么都应该先进行result的更新
                updateCurrentIdKey(log);
                log = judgeUpdate(r, log);
            }
        }
        if (log.op == 'I') {
            result = r.updateInsertInfo(log);
        }
        return result;
    }

    private static Log judgeUpdate(Record r, Log log) {
        long[] _indexes;
        while (log.op != 'I') {
            _indexes = LocalLogIndex.getAllIndexesByPK(log.getCurrentOldKey());
            log = update(r, _indexes);
        }
        return log;
    }

    private static Log update(Record r, long[] _indexes) {
        Log log = null;
        for (long index : _indexes) {
            if (index < r.getLogPosition()) {
                String line = getLineByPosition(LocalLogIndex.getFileNoFromLong(index), LocalLogIndex.getPositionFromLong(index));
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

    private static void updateResult(Record r, Log log, long logPosition) {
        r.updateResult(log);
        r.setLogPosition(logPosition);
    }

}
