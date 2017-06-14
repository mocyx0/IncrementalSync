package org.pangolin.xuzhe;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ubuntu on 17-6-6.
 */
public class Log {
    public char op;
    public ColumnLog[] columns;

    public static void parser(byte[] bytes, int offest, int limit) {
        String str = getString(bytes, offest, limit);
        System.out.println(str);
    }

    public static Log parser(String str) {
        String line = str;
        String[] items = line.split("\\|");
        Log log = new Log();
        log.op = items[5].charAt(0);
        log.columns = ColumnLog.parser(items);
        return log;
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        Map<String, Schema> schemaMap = new HashMap<>();
        byte[] data = ("|mysql-bin.00001717148759|1496736165000|middleware3|student|I|id:1:1|NULL|1|first_name:2:0|NULL|徐|last_name:2:0|NULL|依|sex:2:0|NULL|男|score:1:0|NULL|66|\n" +
                "|mysql-bin.00001717148760|1496736165000|middleware3|student|I|id:1:1|NULL|2|first_name:2:0|NULL|周|last_name:2:0|NULL|诚田|sex:2:0|NULL|女|score:1:0|NULL|56|\n" +
                "|mysql-bin.00001717148761|1496736165000|middleware3|student|I|id:1:1|NULL|3|first_name:2:0|NULL|彭|last_name:2:0|NULL|丁|sex:2:0|NULL|女|score:1:0|NULL|89|\n").getBytes("utf-8");
        int offset = 0;
        int len = getLineLength(data, offset, data.length-offset);
        String line = getString(data, offset, offset + len-1);
        String[] items = line.split("\\|");
        String dbTable = items[3] + "_" + items[4];
        // 根据insert分析出表结构
        if(!schemaMap.containsKey(dbTable) && items[5].equals("I")) {
            Schema schema = Schema.generateFromInsertLog(line);
            schemaMap.put(dbTable, schema);
        }
        Log log = parser(line);
        ArrayList<Log> logList = new ArrayList<>();
        logList.add(log);

        offset += len;
        len = getLineLength(data, offset, data.length);
        line = getString(data, offset, offset + len-1);
        log = parser(line);
        logList.add(log);

        offset += len;
        len = getLineLength(data, offset, data.length);
        line = getString(data, offset, offset + len-1);
        log = parser(line);
        logList.add(log);

    }

    public static int getLineLength(byte[] data, int offset, int limit) {
        byte[] oneLine = new byte[Constants.LINE_MAX_LENGTH];
//        System.err.println("debug:\n"+getString(data, offsetInBlock, limit));
        int len = -1;
        --offset;
        while(++offset < limit && (oneLine[++len] = data[offset]) != '\n')
//            System.out.println((char)oneLine[len]);
            ;  // no op
        return len+1;
    }


//    public static void main(String[] args) throws UnsupportedEncodingException {
//        long itCnt = 1000000;
////        boolean printInfo = false;
//        boolean printInfo = true;
//        long begin = System.nanoTime();
//        Pattern p = Pattern.compile("|");
//        for(int i = 0; i < itCnt; i++) {
//            String n = new String("|000001:106|1489133349000|test|user|I|id:1:1|NULL|102|name:2:0|NULL|ljh|score:1:0|NULL|98|");
////            p.split(n);
//        }
//
//        long end = System.nanoTime();
//        System.out.println("elapsed time: " + (end-begin)/1000000.0 + "ms");
//        byte[] log1 = "|000001:106|1489133349000|test|user|I|id:1:1|NULL|102|name:2:0|NULL|ljh|score:1:0|NULL|98|".getBytes("utf-8");
//        byte[] dbName = new byte[100];
//        begin = System.nanoTime();
////        itCnt = 1;
//        for(int it = 0; it < itCnt; it++) {
//            parseLog(log1, 0, log1.length);
//        }
//        end = System.nanoTime();
//        System.out.println("elapsed time: " + (end-begin)/1000000.0 + "ms");
//    }

//    public static Log parseLogDebug(byte[] bytes, int offsetInBlock, int limit) {
//        int next = offsetInBlock, i;
//        if(bytes[next] == '|') ++next;
//        i = findNext(bytes, next, limit);
//        System.out.println(String.format("%2d   binaryID : %s", i, getString(bytes, next, i)));
//        next = i+1;
//        i = findNext(bytes, next, limit);
//        System.out.println(String.format("%2d  timestamp : %s", i, getString(bytes, next, i)));
//        next = i+1;
//        i = findNext(bytes, next, limit);
//        System.out.println(String.format("%2d   database : %s", i, getString(bytes, next, i)));
//        next = i+1;
//        i = findNext(bytes, next, limit);
//        String tableNameStr = getString(bytes, next, i);
//        System.out.println(String.format("%2d      table : %s", i, tableNameStr));
//        next = i+1;
//        i = findNext(bytes, next, limit);
//        String opType = getString(bytes, next, i);
//        System.out.println(String.format("%2d         op : %s", i, opType));
//        next = i+1;
//        ColumnLog log = new ColumnLog();
//        next = log.parser(bytes, next, limit) + 1;
//        System.out.println(String.format("%2d columnInfo : %s", next, log));
//        log = new ColumnLog();
//        next = log.parser(bytes, next, limit) + 1;
//        System.out.println(String.format("%2d columnInfo : %s", next, log));
//        log = new ColumnLog();
//        next = log.parser(bytes, next, limit) + 1;
//        System.out.println(String.format("%2d columnInfo : %s", next, log));
//        return null;
//    }
//
//    public static Log parseLog(byte[] bytes, int offsetInBlock, int limit) {
//        Log log = new Log();
//        int next = offsetInBlock, i;
//        if(bytes[next] == '|') ++next;
//        next = findNext(bytes, next, limit) + 1; // binaryID
//        next = findNext(bytes, next, limit) + 1; // timestamp
//        next = findNext(bytes, next, limit) + 1; // database
//        next = findNext(bytes, next, limit) + 1; // table
//        byte op = bytes[next];
//        next += 2;
//        ColumnLog columnLog = new ColumnLog();
//        next = columnLog.parser(bytes, next, limit) + 1;
//        columnLog = new ColumnLog();
//        next = columnLog.parser(bytes, next, limit) + 1;
//        columnLog = new ColumnLog();
//        next = columnLog.parser(bytes, next, limit) + 1;
//        return null;
//    }
//
//    /**
//     * 搜索bytes数组中[offsetInBlock, limit）范围内，以|分割后的第一个字符串，返回字符串尾部的下一个位置在bytes中的索引
//     * @param bytes 源数据
//     * @param offsetInBlock 起始搜索位置
//     * @param limit 搜索终止位置，不搜索该位置
//     * @return 终止符的位置，一般为'|'的位置
//     */
//    public static int findNext(byte[] bytes, int offsetInBlock, int limit) {
//        while(++offsetInBlock < limit && bytes[offsetInBlock] != '|')
//            ;
//        return offsetInBlock;
//    }

    public static String getString(byte[] bytes, int offset, int limit) {
        try {
            return new String(bytes, offset, limit - offset, "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
