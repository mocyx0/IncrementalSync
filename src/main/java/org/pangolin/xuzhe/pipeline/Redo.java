package org.pangolin.xuzhe.pipeline;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 29146 on 2017/6/14.
 */
public class Redo {
    private long beginPk;
    private long endPk;
    private Map<Long, Record> pkMap = new HashMap<>();
    public Redo(long beginPk, long endPk){
        this.beginPk = beginPk;
        this.endPk = endPk;
    }

    public Record redo(String logInfo){
        Record r = null;
        Log log = Log.parser(logInfo);
//        if(log.op == 'D'){
//            if(!pkMap.containsKey(log.columns[0].oldLongValue))
//                pkMap.put(log.columns[0].oldLongValue, null);
//        }
        if(log.op != 'D'){
            long newPk = log.columns[0].newLongValue;
            if((newPk <= beginPk || newPk >= endPk) && !pkMap.containsKey(newPk)){
                return r;
            }
        }
        if (log.op == 'U') {
            updateCurrentIdKey(log);
            r = pkMap.get(log.getCurrentNewkey());
            if(r == null){
                r = Record.createFromLastLog(log);
                pkMap.put(log.getCurrentNewkey(),r);
            }
            r.updateResult(log);
            if(log.getCurrentOldKey() != log.getCurrentNewkey()){
                pkMap.remove(log.getCurrentNewkey());
                pkMap.put(log.getCurrentOldKey(),r);
            }
        }
        if(log.op == 'I'){
            r = pkMap.get(log.columns[0].newLongValue);
            if(r == null){
                r = Record.createFromLastLog(log);
                pkMap.put(log.getCurrentNewkey(),r);
            }
            r.updateResult(log);
            r.setLog(log);
//            resualt = r.updateInsertInfo(log);
            pkMap.remove(log.columns[0].newLongValue);
            return r;
        }
        return null;
    }


    private void updateCurrentIdKey(Log log){
        for (ColumnLog columnLog : log.columns) {
            if (columnLog.columnInfo.isPK) {
                log.setCurrentOldKey(columnLog.oldLongValue);
                log.setCurrentNewkey(columnLog.newLongValue);
                break;
            }
        }
    }

}
