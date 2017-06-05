package org.pangolin.yx;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.*;

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
    private TreeMap<Long, ArrayList<LogInfo>> logRebuild = new TreeMap<>();

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
                    //从后向前分析
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
                        //记录被删除
                        break;
                    }
                }
            } else {
                break;
            }
        }
        return re;
    }

    TableInfo getTableInfo(QueryData query) {
        String hashStr = query.scheme + " " + query.table;
        TableInfo block = null;
        if (logindex.tableInfos.containsKey(hashStr)) {
            block = logindex.tableInfos.get(hashStr);
        }
        return block;
    }

    LogBlock getLogBlock(QueryData query) {
        String hashStr = query.scheme + " " + query.table;
        LogBlock block = null;
        if (logindex.logInfos.containsKey(hashStr)) {
            block = logindex.logInfos.get(hashStr);
        }
        return block;
    }

    //搜集log信息  id对应一个log记录
    private void getLogInfo(QueryData query) throws Exception {
        String hashStr = query.scheme + " " + query.table;
        LogBlock block = getLogBlock(query);
        if (block == null) {
            return;
        } else {
            for (long i = query.start + 1; i < query.end; i++) {
                ArrayList<LogInfo> infos = getLogs(block, i);
                logRebuild.put(i, infos);
            }
        }
    }

    HashMap<String, RandomAccessFile> rafs = new HashMap<>();

    private RandomAccessFile getLogFile(String path) throws Exception {
        if (!rafs.containsKey(path)) {
            rafs.put(path, new RandomAccessFile(path, "r"));
        }
        return rafs.get(path);
    }

    //读取实际的物理信息
    private void getAllLog() throws Exception {
        //逐条读取
        for (Map.Entry<Long, ArrayList<LogInfo>> kv : logRebuild.entrySet()) {
            Long id = kv.getKey();
            ArrayList<LogInfo> logs = kv.getValue();
            for (LogInfo v : logs) {
                RandomAccessFile raf = getLogFile(v.logPath);
                Util.fillLogData(raf, v);
            }
        }
    }

    //读取
    private RebuildResult rebuildData(QueryData query) throws Exception {
        RebuildResult re = new RebuildResult();

        for (Map.Entry<Long, ArrayList<LogInfo>> kv : logRebuild.entrySet()) {
            Long id = kv.getKey();
            ArrayList<LogInfo> logs = kv.getValue();
            //get table meta
            TableInfo tinfo = getTableInfo(query);
            int dataCount = tinfo.columns.size();
            HashMap<String, String> values = new HashMap<>();
            //read log
            for (LogInfo v : logs) {
                if (values.size() != dataCount) {
                    //这里应该只会出现update->update->insert的结构,所以每个newvalue都是有意义的
                    for (ParserColumnInfo colInfo : v.columns) {
                        if (!values.containsKey(colInfo.name)) {
                            values.put(colInfo.name, colInfo.newValue);
                        }
                    }
                } else {
                    break;
                }
            }
            if (values.size() == 0) {

            } else if (values.size() != dataCount) {
                throw new Exception("column count error");
            } else {
                ArrayList<String> datas = new ArrayList<>();
                for (String colName : tinfo.columns) {
                    datas.add(values.get(colName));
                }
                re.datas.add(datas);
            }
        }
        return re;
    }

    public RebuildResult getResult(QueryData query) throws Exception {
        getLogInfo(query);
        getAllLog();
        return rebuildData(query);
    }
}
