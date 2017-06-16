package org.pangolin.yx.zhengxu;

import org.pangolin.xuzhe.Log;
import org.pangolin.yx.Config;
import org.slf4j.Logger;

import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by yangxiao on 2017/6/16.
 */


public class LineParserDirect {
    private static Logger logger;
    public static TableInfo tableInfo = new TableInfo();
    private static long lineCount = 0;

    static {
        logger = Config.serverLogger;
    }

    private static int BUFFER_SIZE = 1024 * 32;
    private static ArrayList<RandomAccessFile> rafs = new ArrayList<>();
    private static ArrayList<Long> fileSizes = new ArrayList<>();
    private static int fileIndex = 0;

    private static long offInFile = 0;
    private static byte[] buffer = new byte[BUFFER_SIZE];
    private static int bufferLimit = 0;
    private static int bufferReadPos = 0;

    public static void init(ArrayList<String> files) throws Exception {
        for (String s : files) {
            RandomAccessFile raf = new RandomAccessFile(s, "r");
            rafs.add(raf);
            fileSizes.add(raf.length());
        }
    }

    //返回token的长度
    private static int nextToken(byte[] data, int off, char delimit) throws Exception {
        int end = off;
        boolean sucess = false;
        while (end < bufferLimit) {
            if (data[end] == delimit) {
                sucess = true;
                break;
            }
            end++;
        }

        if (sucess) {
            return end - off;
        } else {
            throw new BufferUnderflowException();
        }
    }

    //parse the fisrt log to get table info
    private static void parseTableInfo() throws Exception {
        ArrayList<byte[]> columns = new ArrayList<>();
        byte[] data = buffer;
        int pos = bufferReadPos;
        pos = pos + 1 + nextToken(data, pos, '|');
        pos = pos + 1 + nextToken(data, pos, '|');//uid
        pos = pos + 1 + nextToken(data, pos, '|');//time
        pos = pos + 1 + nextToken(data, pos, '|');//scheme
        pos = pos + 1 + nextToken(data, pos, '|');//table
        int opPos = pos;
        byte op = data[opPos];
        pos = pos + 1 + nextToken(data, pos, '|');//op

        while (true) {

            if (data[pos] == '\n') {
                pos += 1;
                break;
            }
            int namePos = pos;
            int nameLen = nextToken(data, pos, ':');//col name
            pos += 1 + nameLen;
            pos = pos + 1 + nextToken(data, pos, ':');//type
            pos = pos + 1 + nextToken(data, pos, '|');//pk
            byte type = data[namePos + nameLen + 1];
            byte isPk = data[namePos + nameLen + 3];
            pos = pos + 1 + nextToken(data, pos, '|');//old value
            pos = pos + 1 + nextToken(data, pos, '|');//new value
            if (isPk == '1') {
                tableInfo.pkName = new byte[nameLen];
                System.arraycopy(data, namePos, tableInfo.pkName, 0, nameLen);
            }
            byte[] name = new byte[nameLen];
            System.arraycopy(data, namePos, name, 0, nameLen);
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

    private static LogRecord parseLineReal() throws Exception {
        byte[] data = buffer;
        LogRecord logRecord = new LogRecord();
        logRecord.lineData = data;
        logRecord.columnData = new short[3 * (tableInfo.columnName.length - 1)];
        int colWriteIndex = 0;

        int pos = bufferReadPos;
        pos = pos + 1 + nextToken(data, pos, '|');
        pos = pos + 1 + nextToken(data, pos, '|');//uid
        pos = pos + 1 + nextToken(data, pos, '|');//time
        pos = pos + 1 + nextToken(data, pos, '|');//scheme
        pos = pos + 1 + nextToken(data, pos, '|');//table
        int opPos = pos;
        pos = pos + 1 + nextToken(data, pos, '|');//op
        byte op = data[opPos];
        logRecord.opType = op;

        while (true) {
            if (pos >= bufferLimit) {
                throw new BufferUnderflowException();
            } else if (data[pos] == '\n') {
                //read end
                pos += 1;
                break;
            }
            int namePos = pos;
            int nameLen = nextToken(data, pos, ':');//col name
            pos += 1 + nameLen;
            pos = pos + 1 + nextToken(data, pos, ':');//type
            pos = pos + 1 + nextToken(data, pos, '|');//pk
            byte type = data[namePos + nameLen + 1];
            byte isPk = data[namePos + nameLen + 3];
            int oldPos = pos;
            int oldLen = nextToken(data, pos, '|');
            pos = pos + 1 + oldLen;//old value
            int newPos = pos;
            int newLen = nextToken(data, pos, '|');
            pos = pos + 1 + newLen;//new value
            if (isPk == '1') {
                if (op == 'I') {
                    logRecord.id = parseLong(data, newPos, newLen);
                } else if (op == 'D') {
                    logRecord.preId = parseLong(data, oldPos, oldLen);
                } else if (op == 'U') {
                    logRecord.id = parseLong(data, newPos, newLen);
                    logRecord.preId = parseLong(data, oldPos, oldLen);
                }
            } else {
                int byteIndex = tableInfo.getColumnIndex(data, namePos, nameLen);
                logRecord.columnData[colWriteIndex++] = (short) byteIndex;
                logRecord.columnData[colWriteIndex++] = (short) newPos;
                logRecord.columnData[colWriteIndex++] = (short) newLen;
            }

        }

        int len = pos - bufferReadPos;
        bufferReadPos = pos;
        offInFile += len;
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

    private static void nextBlock() {
        bufferReadPos = 0;

    }

    public static LogRecord nextLine() throws Exception {

        lineCount++;
        if (lineCount == 1) {
            //read first block
            RandomAccessFile raf = rafs.get(fileIndex);
            int len = raf.read(buffer);
            bufferLimit = len;
            bufferReadPos = 0;
            //
            parseTableInfo();
        }
        LogRecord re;
        try {
            re = parseLineReal();
        } catch (BufferUnderflowException e) {
            if (offInFile < fileSizes.get(fileIndex)) {

            } else {
                //next file
                fileIndex++;
                offInFile = 0;
                if (fileIndex >= fileSizes.size()) {
                    return null;
                }
            }
            //read a block
            buffer = new byte[BUFFER_SIZE];
            RandomAccessFile raf = rafs.get(fileIndex);
            raf.seek(offInFile);
            int len = raf.read(buffer);
            bufferLimit = len;
            bufferReadPos = 0;
            re = parseLineReal();
        }

        re.seq = lineCount;
        return re;
    }

}
