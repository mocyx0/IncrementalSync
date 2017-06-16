package org.pangolin.yx.zhengxu;

import org.pangolin.xuzhe.Log;
import org.pangolin.yx.Config;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by yangxiao on 2017/6/16.
 */

class LogRecord {
    byte[] lineData;
    long id = -1;
    long preId = -1;
    byte opType;
    short[] columnData;//两个字节一个单位 位置+长度  位置=0表示结束
    long seq;//log sequence
}

class TableInfo {
    byte[][] columnName;//包含pk
    byte[] pkName;

    int getColumnIndex(byte[] data, int off, int len) {
        for (int i = 0; i < columnName.length; i++) {
            boolean equal = true;
            for (int j = 0; j < len; j++) {
                if (j < columnName[i].length && columnName[i][j] == data[off + j]) {

                } else {
                    equal = false;
                    break;
                }
            }
            if (equal) {
                return i;
            }
        }
        return -1;
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
    private static Logger logger;
    public static TableInfo tableInfo = new TableInfo();
    private static long lineCount = 0;
    private static LineReader lineReader;

    static {
        logger = Config.serverLogger;
    }

    public static void init(ArrayList<String> paths) throws Exception {
        lineReader = new LineReader(paths);
    }

    public static LogRecord nextLine() throws Exception {
        LineInfo lineInfo = lineReader.nextLine();
        if (lineInfo != null) {
            return parseLine(lineInfo);
        } else {
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
        logger.info(sb.toString());
        tableInfo.columnName = new byte[columns.size()][];
        for (int i = 0; i < columns.size(); i++) {
            tableInfo.columnName[i] = columns.get(i);
        }
    }

    private static long parseLong(byte[] data, int off, int len) {
        long v = 0;
        for (int i = 0; i < len; i++) {
            v = v * 10 + (data[off + i] - '0');
        }
        return v;
    }

    private static LogRecord parseLineReal(LineInfo lineInfo) {
        LogRecord logRecord = new LogRecord();
        logRecord.lineData = lineInfo.data;
        logRecord.columnData = new short[3 * (tableInfo.columnName.length - 1)];
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
                }
            } else {
                int byteIndex = tableInfo.getColumnIndex(lineInfo.data, namePos, nameLen);
                logRecord.columnData[colWriteIndex++] = (short) byteIndex;
                logRecord.columnData[colWriteIndex++] = (short) newPos;
                logRecord.columnData[colWriteIndex++] = (short) newLen;
            }
        }
        return logRecord;
    }

    private static void printLogRecord(LogRecord logRecord) {
        String op = "" + ((char) logRecord.opType);
        logger.info(String.format("%s %d %d", op, logRecord.preId, logRecord.id));
        StringBuilder colValue = new StringBuilder();
        for (int i = 0; i < logRecord.columnData.length / 3; i++) {
            short name = logRecord.columnData[i * 3];
            short pos = logRecord.columnData[i * 3 + 1];
            short len = logRecord.columnData[i * 3 + 2];
            if (pos == 0) {
                break;
            }
            String value = new String(logRecord.lineData, pos, len);
            colValue.append(value);
            colValue.append(" ");
        }
        logger.info(colValue.toString());
    }

    private static LogRecord parseLine(LineInfo lineInfo) {


        lineCount++;
        if (lineCount == 1) {
            parseTableInfo(lineInfo);

        }
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
