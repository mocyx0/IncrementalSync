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

        if(log.op == 'D') {
            return r;
        }else{
            long newPk = log.columns[0].newLongValue;
            if((newPk <= beginPk || newPk >= endPk) && !pkMap.containsKey(newPk)){
                return r;
            }
            if (log.op == 'U') {
                long oldPk = log.columns[0].oldLongValue;
                r = updateRecord(log, newPk);
                if(oldPk != newPk){
                    pkMap.remove(newPk);
                    pkMap.put(oldPk,r);
                }
            }
            if(log.op == 'I'){
                r = updateRecord(log, newPk);
                r.setLog(log);
//            resualt = r.updateInsertInfo(log);
                pkMap.remove(newPk);
                return r;
            }
        }
        return null;
    }

    private Record updateRecord(Log log, long newPk) {
        Record r;
        r = pkMap.get(newPk);
        if(r == null){
            r = new Record(newPk);
            pkMap.put(newPk,r);
        }
        r.updateResult(log);
        return r;
    }

}
