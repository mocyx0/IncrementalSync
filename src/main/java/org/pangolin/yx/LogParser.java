package org.pangolin.yx;

import com.sun.org.apache.xerces.internal.impl.xpath.XPath;
import javafx.beans.binding.ObjectExpression;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by yangxiao on 2017/6/4.
 */

//表元信息
class TableInfo {
    String scheme;
    String table;
    String pk;
    //列名
    ArrayList<String> columns = new ArrayList<>();
}

class ParserColumnInfo {
    String name;
    int type;
    String oldValue;
    String newValue;
    int isPk;
}

class StringParser {
    String str;
    int off;

    StringParser(String s, int off) {
        this.str = s;
        this.off = off;
    }

    boolean end() {
        return off >= str.length();
    }
}

class LogBlock {
    HashMap<Long, LinkedList<LogInfo>> idToLogs = new HashMap<>();

    public void checkKey(Long id) {
        if (!idToLogs.containsKey(id)) {
            idToLogs.put(id, new LinkedList<LogInfo>());
        }
    }
}

class LogIndex {
    HashMap<String, TableInfo> tableInfos = new HashMap<>();
    HashMap<String, LogBlock> logInfos = new HashMap<>();
}

class LogInfo {
    public String opType;
    public int preId;
    public int id;
    //file info
    public long offset;
    public String logPath;
    public int length;
    //只有在rebuild时才会有数据
    public ArrayList<ParserColumnInfo> columns = null;
}

public class LogParser {
    LogIndex logIndex = new LogIndex();

    private int insertCount = 0;
    private int updateCount = 0;
    private int deleteCount = 0;

    LineReader lineReader;


    private void parseLine(ReadLineInfo lineInfo) throws Exception {
        String line = lineInfo.line;

        StringParser parser = new StringParser(line, 0);
        String uid = Util.getNextToken(parser, '|');
        String time = Util.getNextToken(parser, '|');
        String scheme = Util.getNextToken(parser, '|');
        String table = Util.getNextToken(parser, '|');
        String op = Util.getNextToken(parser, '|');
        String hashKey = scheme + " " + table;
        //table的第一条insert记录包含所有列, 我们记录下元信息
        if (!logIndex.tableInfos.containsKey(hashKey)) {
            int off = parser.off;
            TableInfo info = new TableInfo();
            info.scheme = scheme;
            info.table = table;
            ParserColumnInfo cinfo = Util.getNextColumnInfo(parser);
            while (cinfo != null) {
                info.columns.add(cinfo.name);
                if (cinfo.isPk == 1) {
                    info.pk = cinfo.name;
                }
                cinfo = Util.getNextColumnInfo(parser);
            }
            logIndex.tableInfos.put(hashKey, info);
            parser.off = off;
        }
        if (!logIndex.logInfos.containsKey(hashKey)) {
            logIndex.logInfos.put(hashKey, new LogBlock());
        }


        //解析到主键为止
        ParserColumnInfo cinfo = Util.getNextColumnInfo(parser);
        while (cinfo != null) {
            if (cinfo.isPk == 1) {
                break;
            }
            cinfo = Util.getNextColumnInfo(parser);
        }
        if (cinfo == null) {
            throw new Exception("no pk");
        }
        Long pkId = null;
        //根据操作的不同  获取对应的主键id
        if (op.equals("I")) {
            pkId = Long.parseLong(cinfo.newValue);
        } else if (op.equals("D")) {
            pkId = Long.parseLong(cinfo.oldValue);
        } else if (op.equals("U")) {
            pkId = Long.parseLong(cinfo.newValue);
        }


        logIndex.logInfos.get(hashKey).checkKey(pkId);
        LinkedList<LogInfo> logs = logIndex.logInfos.get(hashKey).idToLogs.get(pkId);

        LogInfo linfo = new LogInfo();
        linfo.opType = op;
        linfo.logPath = logPath;
        linfo.offset = lineInfo.off;
        linfo.length = lineInfo.length;
        if (op.equals("U")) {
            linfo.id = Integer.parseInt(cinfo.newValue);
            linfo.preId = Integer.parseInt(cinfo.oldValue);
            updateCount++;
        } else if (op.equals("I")) {
            linfo.id = Integer.parseInt(cinfo.newValue);
            insertCount++;
        } else if (op.equals("D")) {
            linfo.id = Integer.parseInt(cinfo.oldValue);
            deleteCount++;
        } else {
            throw new Exception("非法的操作类型");
        }
        //as a queue
        logs.push(linfo);
    }

    String logPath;

    public LogIndex parseLog() throws Exception {
        logPath = Config.DATA_HOME + "/canal.log";
        File f1 = new File(logPath);
        lineReader = new LineReader(logPath, 0, f1.length());

        ReadLineInfo line = lineReader.readLine();
        while (line.line != null) {
            parseLine(line);
            line = lineReader.readLine();
        }
        return logIndex;
    }
}
