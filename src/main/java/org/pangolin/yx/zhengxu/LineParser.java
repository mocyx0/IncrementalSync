package org.pangolin.yx.zhengxu;

import org.pangolin.yx.Config;
import org.pangolin.yx.MLog;
import org.pangolin.yx.PlainHashingSimple;

import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/16.
 */

class LogRecord {
    //   byte[] lineData;
    long id = -1;
    long preId = -1;
    byte opType = 0;
    int[] columnData;//三个一个单位  列索引, 位置, 长度
    long seq;//log sequence
}

class TableInfo {
    byte[][] columnName;//包含pk
    byte[] pkName;

    private PlainHashingSimple hashToIndex = new PlainHashingSimple(8);

    private long hash(byte[] data, int start, int len) {
        long hash = 0;
        for (int j = start; j < len + start; j++) {
            hash = 31 * hash + data[j];
        }
        return hash;
    }

    public void setColumnName(byte[][] columnName) {
        this.columnName = columnName;
        for (int i = 0; i < columnName.length; i++) {
            byte[] name = columnName[i];
            long hash = hash(name, 0, name.length);
            if (hashToIndex.containsKey(hash)) {
                MLog.info("hash ERROR");
            } else {
                hashToIndex.put(hash, i);
            }
        }
    }

    // id first_name last_name sex score score2
    static final int[] indexTable = new int[]{0, 1, 0, 3, 4, 4, 5, 7, 8, 2, 1};

    final int getColumnIndex(byte[] data, int off, int len) throws Exception {
        //if (Config.OPTIMIZE) {
        return indexTable[len];
            /*
        } else {
            long hash = hash(data, off, len);
            return hashToIndex.get(hash);
        }
        */
    }

    boolean byteEqual(byte[] b1, byte[] b2) {
        if (b1.length != b2.length) {
            return false;
        } else {
            for (int i = 0; i < b1.length; i++) {
                if (b1[i] != b2[i]) {
                    return false;
                }
            }
        }
        return true;

    }
}

public class LineParser {
    public static TableInfo tableInfo = new TableInfo();
    public static long lineCount = 0;
    private static LineReader lineReader;
    public static int maxColSize = 0;
    public static int maxLineSize = 0;
    public static long updateCount = 0;
    public static long insertCount = 0;
    public static long deleteCount = 0;
    public static long pkUpdate = 0;

    static {
    }

    public static void init(ArrayList<String> paths) throws Exception {
        lineReader = new LineReader(paths);
    }

    public static LogRecord nextLine() throws Exception {
        LineInfo lineInfo = lineReader.nextLine();
        if (lineInfo != null) {
            maxLineSize = Math.max(maxLineSize, lineInfo.data.length);
            LogRecord re = parseLine(lineInfo);
            //printLogRecord(re);
            return re;
        } else {
            MLog.info(String.format("line:%d insert:%d update:%d delete:%d pkupdate:%d colMaxSize:%d lineMax",
                    lineCount, insertCount, updateCount, deleteCount, pkUpdate, maxColSize, maxLineSize));
            return null;
        }
    }

    //返回token的长度
    private static int nextToken(byte[] data, int off, char delimit) {
        int end = off;
        while (end < data.length) {
            if (data[end] == delimit) {
                break;
            }
            end++;
        }
        return end - off;
    }

    public static void readTableInfo() throws Exception {
        ArrayList<String> path = new ArrayList<>();
        path.add(Config.DATA_HOME + "/1.txt");
        LineReader lineReader = new LineReader(path);
        LineInfo lineInfo = lineReader.nextLine();
        parseTableInfo(lineInfo);
    }

    //parse the fisrt log to get table info
    private static void parseTableInfo(LineInfo lineInfo) {
        ArrayList<byte[]> columns = new ArrayList<>();
        int pos = 0;
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//uid
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//time
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//scheme
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//table
        int opPos = pos;
        byte op = lineInfo.data[opPos];
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//op

        while (pos < lineInfo.data.length) {
            int namePos = pos;
            int nameLen = nextToken(lineInfo.data, pos, ':');//col name
            pos += 1 + nameLen;
            byte type = lineInfo.data[pos];
            byte isPk = lineInfo.data[pos + 2];
            pos += 4;//skip
            pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//old value
            pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//new value
            if (isPk == '1') {
                tableInfo.pkName = new byte[nameLen];
                System.arraycopy(lineInfo.data, namePos, tableInfo.pkName, 0, nameLen);
            }
            byte[] name = new byte[nameLen];
            System.arraycopy(lineInfo.data, namePos, name, 0, nameLen);
            columns.add(name);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("table info: ");
        for (byte[] b : columns) {
            String s = new String(b);
            sb.append(s);
            sb.append(" ");
        }
        MLog.info(sb.toString());
        byte[][] columnName = new byte[columns.size()][];
        for (int i = 0; i < columns.size(); i++) {
            columnName[i] = columns.get(i);
        }
        GlobalData.tableInfo.setColumnName(columnName);
        GlobalData.colCount = GlobalData.tableInfo.columnName.length - 1;
        tableInfo = GlobalData.tableInfo;
    }

    private static long parseLong(byte[] data, int off, int len) {
        long v = 0;
        for (int i = 0; i < len; i++) {
            v = v * 10 + (data[off + i] - '0');
        }
        return v;
    }

    private static LogRecord parseLineReal(LineInfo lineInfo) throws Exception {
        LogRecord logRecord = new LogRecord();
        //logRecord.lineData = lineInfo.data;
        logRecord.columnData = new int[3 * (tableInfo.columnName.length - 1)];
        int colWriteIndex = 0;

        int pos = 0;
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//uid
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//time
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//scheme
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//table
        int opPos = pos;
        byte op = lineInfo.data[opPos];
        logRecord.opType = op;
        pos = pos + 1 + nextToken(lineInfo.data, pos, '|');//op

        if (op == 'I') {
            insertCount++;
        } else if (op == 'U') {
            updateCount++;
        } else if (op == 'D') {
            deleteCount++;
        }

        while (pos < lineInfo.data.length) {
            int namePos = pos;
            int nameLen = nextToken(lineInfo.data, pos, ':');//col name
            pos += 1 + nameLen;
            byte type = lineInfo.data[pos];
            byte isPk = lineInfo.data[pos + 2];
            pos += 4;//skip
            int oldPos = pos;
            int oldLen = nextToken(lineInfo.data, pos, '|');
            pos = pos + 1 + oldLen;//old value
            int newPos = pos;
            int newLen = nextToken(lineInfo.data, pos, '|');
            pos = pos + 1 + newLen;//new value
            if (isPk == '1') {
                if (op == 'I') {
                    logRecord.id = parseLong(lineInfo.data, newPos, newLen);
                } else if (op == 'D') {
                    logRecord.preId = parseLong(lineInfo.data, oldPos, oldLen);
                } else if (op == 'U') {
                    logRecord.id = parseLong(lineInfo.data, newPos, newLen);
                    logRecord.preId = parseLong(lineInfo.data, oldPos, oldLen);
                    if (logRecord.id != logRecord.preId) {
                        pkUpdate++;
                    }
                }
            } else {
                int byteIndex = tableInfo.getColumnIndex(lineInfo.data, namePos, nameLen);
                logRecord.columnData[colWriteIndex++] = (short) byteIndex;
                logRecord.columnData[colWriteIndex++] = (short) newPos;
                logRecord.columnData[colWriteIndex++] = (short) newLen;
                maxColSize = Math.max(newLen, maxColSize);
            }
        }
        return logRecord;
    }
/*
    private static void printLogRecord(LogRecord logRecord) {
        String op = "" + ((char) logRecord.opType);
        logger.info(String.format("%s %d %d", op, logRecord.preId, logRecord.id));
        StringBuilder colValue = new StringBuilder();
        for (int i = 0; i < logRecord.columnData.length / 3; i++) {
            int name = logRecord.columnData[i * 3];
            int pos = logRecord.columnData[i * 3 + 1];
            int len = logRecord.columnData[i * 3 + 2];
            if (pos == 0) {
                break;
            }
            String value = new String(logRecord.lineData, pos, len);
            colValue.append(value);
            colValue.append(" ");
        }
        logger.info(colValue.toString());
    }
    */

    private static LogRecord parseLine(LineInfo lineInfo) throws Exception {


        lineCount++;
        LogRecord re = parseLineReal(lineInfo);
        re.seq = lineCount;
        /*
        if (re.opType == 'D') {
            String s = new String(lineInfo.data);
            logger.info(s);
            printLogRecord(re);
        }
        */

        return re;
    }
}
