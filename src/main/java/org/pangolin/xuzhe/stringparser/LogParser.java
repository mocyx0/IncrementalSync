package org.pangolin.xuzhe.stringparser;

import java.io.IOException;

/**
 * Created by ubuntu on 17-6-7.
 */
public class LogParser {
    public static String schemaName = "middleware3";
    public static String tableName = "student";

    public static void parse(String str, int fileNo, int position, LocalLogIndex indexes) {
        try {
            String line = str;
            String[] items = line.split("\\|");
            if (!getDatabaseName(items).equals(schemaName) || !getTableName(items).equals(tableName)) {
                return;
            }
            long pk = getPK(items);
            indexes.appendIndex(pk, getTimestamp(items), fileNo, position);
        } catch (Exception e) {
            System.out.println(str);
        }
    }

    public static long getTimestamp(String[] items) {
        return Long.parseLong(items[2]);
    }
    public static String getDatabaseName(String[] items) {
        return items[3];
    }
    public static String getTableName(String[] items) {
        return items[4];
    }
    public static String getOpType(String[] items) {
        return items[5];
    }
    // TODO 暂时假设主键在日志中是第一列，后期查看canel生成日志的源码验证
    public static long getPK(String[] items) {
        String pk;
        if(getOpType(items).charAt(0) == 'D') {
            pk = getColumnAllInfoByIndex(items, 0)[1];

        } else {
            pk = getColumnAllInfoByIndex(items, 0)[2];
        }
        return Long.parseLong(pk);
    }
    public static String[] getAllColumn(String[] items) {
        int cnt = getColumnCount(items);
        String[] result = new String[cnt];
        for(int i = 0; i < cnt; i++) {
            result[i] = items[6+i*3];
        }
        return result;
    }
    public static int getColumnCount(String[] items) {
        return (items.length - 6) / 3;
    }
    public static String[] getColumnAllInfoByIndex(String[] items, int index) {
        String[] result = new String[3];
        result[0] = items[6+3*index];
        result[1] = items[7+3*index];
        result[2] = items[8+3*index];
        return result;
    }

    public static String[] getColumnMetaInfo(String str) {
        return str.split(":");
    }
}
