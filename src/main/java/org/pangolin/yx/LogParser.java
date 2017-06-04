package org.pangolin.yx;

import com.sun.org.apache.xerces.internal.impl.xpath.XPath;
import javafx.beans.binding.ObjectExpression;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.RandomAccessFile;
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


class LogInfo {
    public int preId;
    public String opType;
    public int id;
    public long offset;
    public String logPath;
}

public class LogParser {

    private HashMap<String, TableInfo> tableInfos = new HashMap<>();
    private HashMap<String, LogBlock> logInfos = new HashMap<>();
    private int insertCount = 0;
    private int updateCount = 0;
    private int deleteCount = 0;

    private String getNextToken(StringParser parser, char delimit) {
        int s = parser.off;
        while (s < parser.str.length() && parser.str.charAt(s) == delimit) {
            s++;
        }
        int e = s;
        while (e < parser.str.length() && parser.str.charAt(e) != delimit) {
            e++;
        }
        parser.off = e + 1;
        return parser.str.substring(s, e);

    }

    private ParserColumnInfo getNextColumnInfo(StringParser parser) {
        if (parser.end()) {
            return null;
        }
        ParserColumnInfo info = new ParserColumnInfo();
        info.name = getNextToken(parser, ':');
        info.type = Integer.parseInt(getNextToken(parser, ':'));
        info.isPk = Integer.parseInt(getNextToken(parser, '|'));
        info.oldValue = getNextToken(parser, '|');
        info.newValue = getNextToken(parser, '|');
        return info;
    }

    private void parseLine(String line, long fileOff) throws Exception {
        StringParser parser = new StringParser(line, 0);
        String uid = getNextToken(parser, '|');
        String time = getNextToken(parser, '|');
        String scheme = getNextToken(parser, '|');
        String table = getNextToken(parser, '|');
        String op = getNextToken(parser, '|');
        String hashKey = scheme + " " + table;
        //table的第一条insert记录包含所有列, 我们记录下元信息
        if (!tableInfos.containsKey(hashKey)) {
            int off = parser.off;
            TableInfo info = new TableInfo();
            info.scheme = scheme;
            info.table = table;
            ParserColumnInfo cinfo = getNextColumnInfo(parser);
            while (cinfo != null) {
                info.columns.add(cinfo.name);
                if (cinfo.isPk == 1) {
                    info.pk = cinfo.name;
                }
                cinfo = getNextColumnInfo(parser);
            }
            tableInfos.put(hashKey, info);
            parser.off = off;
        }
        if (!logInfos.containsKey(hashKey)) {
            logInfos.put(hashKey, new LogBlock());
        }


        //解析到主键为止
        ParserColumnInfo cinfo = getNextColumnInfo(parser);
        while (cinfo != null) {
            if (cinfo.isPk == 1) {
                break;
            }
            cinfo = getNextColumnInfo(parser);
        }
        if (cinfo == null) {
            System.out.print(1);
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


        logInfos.get(hashKey).checkKey(pkId);
        LinkedList<LogInfo> logs = logInfos.get(hashKey).idToLogs.get(pkId);

        LogInfo linfo = new LogInfo();
        linfo.opType = op;
        linfo.logPath = "";
        linfo.offset = fileOff;
        if (op.equals("U")) {
            linfo.id = Integer.parseInt(cinfo.newValue);
            linfo.preId = Integer.parseInt(cinfo.oldValue);
            updateCount++;
        } else if (op.equals("I")) {
            linfo.id = Integer.parseInt(cinfo.newValue);
            insertCount++;
        } else if (op.equals("D")) {
            deleteCount++;
        } else {
            throw new Exception("非法的操作类型");
        }
        logs.add(linfo);
    }

    public void parseLog() throws Exception {
        String logPath = Config.DATA_HOME + "/canal.log";
        RandomAccessFile raf = new RandomAccessFile(logPath, "r");
        BufferedReader fr = new BufferedReader(new FileReader(raf.getFD()));

        String line = fr.readLine();
        while (line != null) {
            parseLine(line, 0);
            line = fr.readLine();
        }
    }

    public List<Record> getResult() {
        return null;
    }
}
