package org.pangolin.yx;

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
    private AliLogData aliLogData;
    //需要rebuild的log
    private TreeMap<Long, ArrayList<LogRecord>> logRebuild = new TreeMap<>();

    public LogRebuilder(AliLogData aliLogData) {
        this.aliLogData = aliLogData;
    }

    //根据id在blocks上找出对应的日志记录
    ArrayList<LogRecord> getLogs(QueryData queryData, long id) throws Exception {



        ArrayList<LogRecord> re = new ArrayList<>();
        String hashKey = queryData.scheme + " " + queryData.table;

        int blockIndex = aliLogData.blockLogs.size() - 1;
        if (id == 4) {
            System.out.print(1);
        }

        Long targetId = id;
        while (true) {
            if (targetId == null) {
                break;
            }
            if (blockIndex < 0) {
                break;
            }
            //反向遍历
            BlockLog blockLog = aliLogData.blockLogs.get(blockIndex);
            if (blockLog.logInfos.containsKey(hashKey)) {
                LogOfTable logOfTable = blockLog.logInfos.get(hashKey);

                //
                if(logOfTable.isDeleted(targetId)){
                    return re;
                }
                LogRecord lastLog = logOfTable.getLogById(targetId);
                //
                while (lastLog != null) {
                    re.add(lastLog);
                    if (lastLog.preLogIndex != -1) {
                        lastLog = logOfTable.getLog(lastLog.preLogIndex);
                    } else {
                        targetId = lastLog.preId;
                        break;
                    }
                }
                /*
                while (true) {
                    if (!logOfTable.idToLogs.containsKey(id)) {
                        break;
                    } else {
                        //获取队列
                        LinkedList<LogRecord> logs = logOfTable.idToLogs.get(id);
                        if (logs.size() == 0) {
                            break;
                        } else {
                            //从后向前分析
                            LogRecord info = logs.poll();
                            if (info.opType.equals("U")) {
                                //主键update
                                id = info.preId;
                                re.add(info);
                            } else if (info.opType.equals("I")) {
                                //意味着这是第一条记录
                                re.add(info);
                                return re;
                            } else if (info.opType.equals("D")) {
                                //记录被删除
                                return re;
                            }
                        }
                    }

                }
                */
            }
            blockIndex--;
        }
        return re;
    }

    TableInfo getTableInfo(QueryData query) {
        String hashStr = query.scheme + " " + query.table;
        TableInfo block = null;
        if (aliLogData.tableInfos.containsKey(hashStr)) {
            block = aliLogData.tableInfos.get(hashStr);
        }
        return block;
    }


    //搜集log信息  id对应一个log记录
    private void getLogInfo(QueryData query) throws Exception {
        String hashStr = query.scheme + " " + query.table;
        for (long i = query.start + 1; i < query.end; i++) {
            ArrayList<LogRecord> infos = getLogs(query, i);
            logRebuild.put(i, infos);
        }

        /*
        LogOfTable block = getLogBlock(query);
        if (block == null) {
            return;
        } else {

        }
        */
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
        for (Map.Entry<Long, ArrayList<LogRecord>> kv : logRebuild.entrySet()) {
            Long id = kv.getKey();
            ArrayList<LogRecord> logs = kv.getValue();
            for (LogRecord v : logs) {
                RandomAccessFile raf = getLogFile(v.logPath);
                Util.fillLogData(raf, v);
            }
        }
    }

    //读取
    private RebuildResult rebuildData(QueryData query) throws Exception {
        RebuildResult re = new RebuildResult();
        for (Map.Entry<Long, ArrayList<LogRecord>> kv : logRebuild.entrySet()) {
            Long id = kv.getKey();
            ArrayList<LogRecord> logs = kv.getValue();
            //get table meta
            TableInfo tinfo = getTableInfo(query);
            int dataCount = tinfo.columns.size();
            HashMap<String, String> values = new HashMap<>();
            //read log
            for (LogRecord v : logs) {
                if (values.size() != dataCount) {
                    //这里应该只会出现update->update->insert的结构,所以每个newvalue都是有意义的
                    for (LogColumnInfo colInfo : v.columns) {
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
