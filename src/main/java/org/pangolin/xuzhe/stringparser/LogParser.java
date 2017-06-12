package org.pangolin.xuzhe.stringparser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Created by ubuntu on 17-6-7.
 */
public class LogParser {
    public static String schemaName = "middleware5";
    public static String tableName = "student";

    public static void parseToIndex(String str, int fileNo, int position, ArrayList<String> out) {
        try {
            String line = str;
            out.clear();
            StringTokenizer tokenizer = new StringTokenizer(str, "|", false);
            while (tokenizer.hasMoreElements()) {
                out.add(tokenizer.nextToken());
            }

//            String[] items = line.split("\\|");
            if (!getDatabaseName(out).equals(schemaName) || !getTableName(out).equals(tableName)) {
                return;
            }
            long pk = getPK(out);
//            indexes.appendIndex(pk, fileNo, position);
            LocalLogIndex.appendIndex2(pk, fileNo, position);
        } catch (Exception e) {
            System.out.println(str);
        }
    }

    public static long getTimestamp(ArrayList<String> items) {
        return Long.parseLong(items.get(2));
    }

    public static String getDatabaseName(ArrayList<String> items) {
        return items.get(3);
    }

    public static String getTableName(ArrayList<String> items) {
        return items.get(4);
    }

    public static String getOpType(ArrayList<String> items) {
        return items.get(5);
    }

    // TODO 暂时假设主键在日志中是第一列，后期查看canel生成日志的源码验证
    public static long getPK(ArrayList<String> items) {
        String pk;
        if(getOpType(items).charAt(0) == 'D') {
            pk = getColumnAllInfoByIndex(items, 0)[1];

        } else {
            pk = getColumnAllInfoByIndex(items, 0)[2];
        }
        return Long.parseLong(pk);
    }

    public static String[] getAllColumn(ArrayList<String> items) {
        int cnt = getColumnCount(items);
        String[] result = new String[cnt];
        for(int i = 0; i < cnt; i++) {
            result[i] = items.get(6+i*3);
        }
        return result;
    }

    public static int getColumnCount(ArrayList<String> items) {
        return (items.size() - 6) / 3;
    }

    public static String[] getColumnAllInfoByIndex(ArrayList<String> items, int index) {
        String[] result = new String[3];
        result[0] = items.get(6+3*index);
        result[1] = items.get(7+3*index);
        result[2] = items.get(8+3*index);
        return result;
    }

    public static String[] getColumnMetaInfo(String str) {
        return str.split(":");
    }
}
