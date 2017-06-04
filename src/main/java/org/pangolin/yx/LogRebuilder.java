package org.pangolin.yx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by yangxiao on 2017/6/4.
 */

class RebuildResult {
    ArrayList<ArrayList<String>> datas = new ArrayList<>();
}

class QueryData {
    String scheme;
    String table;
    long start;
    long end;
}

public class LogRebuilder {
    private LogIndex logindex;
    //需要rebuild的log
    private HashMap<Long, ArrayList<LogInfo>> logRebuild = new HashMap<>();

    public LogRebuilder(LogIndex logindex) {
        this.logindex = logindex;
    }


    ArrayList<LogInfo> getLogs(LogBlock block, long id) throws Exception {
        ArrayList<LogInfo> re = new ArrayList<>();
        while (true) {
            if (block.idToLogs.containsKey(id)) {
                LinkedList<LogInfo> logs = block.idToLogs.get(id);
                if (logs.size() == 0) {
                    break;
                } else {
                    LogInfo info = logs.poll();
                    if (info.opType.equals("U")) {
                        //主键update
                        id = info.preId;
                        re.add(info);
                    } else if (info.opType.equals("I")) {
                        //意味着这是第一条记录
                        re.add(info);
                        break;
                    } else if (info.opType.equals("D")) {
                        //不可能从delete开始
                        throw new Exception("wrong op");
                    }
                }
            } else {
                break;
            }
        }
        return re;
    }


    //搜集log信息  id对应一个log记录
    private void getLogInfo(QueryData query) throws Exception {
        String hashStr = query.scheme + " " + query.table;
        LogBlock block = null;
        if (logindex.logInfos.containsKey(hashStr)) {
            block = logindex.logInfos.get(hashStr);
        }
        if (block == null) {
            return;
        } else {
            for (long i = query.start; i < query.end; i++) {
                ArrayList<LogInfo> infos = getLogs(block, i);
                logRebuild.put(i, infos);
            }
        }
    }

    //读取实际的物理信息
    private void getAllLog() {
        //逐条读取
        for (Map.Entry<Long, ArrayList<LogInfo>> kv : logRebuild.entrySet()) {
            Long id = kv.getKey();
            ArrayList<LogInfo> logs = kv.getValue();
            for (LogInfo v : logs) {

            }
        }
    }

    //读取
    private RebuildResult rebuildData() {
        return null;
    }

    public RebuildResult getResult(QueryData query) throws Exception {
        getLogInfo(query);
        getAllLog();
        return rebuildData();
    }
}
