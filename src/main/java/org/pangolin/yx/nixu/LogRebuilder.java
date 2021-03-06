package org.pangolin.yx.nixu;

import org.pangolin.yx.Config;
import org.pangolin.yx.Util;

import java.io.RandomAccessFile;
import java.util.*;

/**
 * Created by yangxiao on 2017/6/4.
 */

class RebuildResult {
    ArrayList<ArrayList<String>> datas = new ArrayList<>();
}

public class LogRebuilder {
    private AliLogData aliLogData;
    //需要rebuild的log
    private TreeMap<Long, ArrayList<LogRecord>> logRebuild = new TreeMap<>();

    public LogRebuilder(AliLogData aliLogData) {
        this.aliLogData = aliLogData;
    }

    //根据id在blocks上找出对应的日志记录
    ArrayList<LogRecord> getLogs(long id) throws Exception {
        ArrayList<LogRecord> re = new ArrayList<>();
        //String hashKey = queryData.scheme + " " + queryData.table;

        int blockIndex = aliLogData.blockLogs.size() - 1;
        Long targetId = id;
        while (true) {
            //主键不会出现为-1 所以可以用-1来判断
            if (targetId == -1) {
                break;
            }
            if (blockIndex < 0) {
                break;
            }
            //反向遍历
            BlockLog blockLog = aliLogData.blockLogs.get(blockIndex);
            LogOfTable logOfTable = blockLog.logOfTable;
            //是否被删除?(标记为-1)
            if (logOfTable.isDeleted(targetId)) {
                return re;
            }
            //获取上一条日志
            LogRecord lastLog = logOfTable.getLogById(targetId);
            //
            while (lastLog != null) {
                LogParser.fillFileInfo(lastLog,blockLog);

                re.add(lastLog);
                if (lastLog.preLogOff != -1) {
                    lastLog = logOfTable.getLog(lastLog.preLogOff);
                } else {
                    //这是单个block的第一条日志,上一条日志需要根据preid去查找
                    RandomAccessFile raf = getLogFile(lastLog.logPath);
                    NXUtil.fillLogData(raf, lastLog);
                    targetId = lastLog.preId;
                    break;
                }
            }
            blockIndex--;
        }
        return re;
    }

    //搜集log信息  id对应一个log记录
    private void getLogInfo() throws Exception {
        for (long i = Config.queryData.start + 1; i < Config.queryData.end; i++) {
            ArrayList<LogRecord> infos = getLogs(i);
            logRebuild.put(i, infos);
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
        for (Map.Entry<Long, ArrayList<LogRecord>> kv : logRebuild.entrySet()) {
            Long id = kv.getKey();
            ArrayList<LogRecord> logs = kv.getValue();
            for (LogRecord v : logs) {
                RandomAccessFile raf = getLogFile(v.logPath);
                NXUtil.fillLogData(raf, v);
            }
        }
    }

    //读取
    private RebuildResult rebuildData() throws Exception {
        RebuildResult re = new RebuildResult();
        for (Map.Entry<Long, ArrayList<LogRecord>> kv : logRebuild.entrySet()) {
            Long id = kv.getKey();
            ArrayList<LogRecord> logs = kv.getValue();
            //get table meta
            TableInfo tinfo = aliLogData.tableInfo;
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

    public RebuildResult getResult() throws Exception {
        getLogInfo();
        getAllLog();
        return rebuildData();
    }
}
