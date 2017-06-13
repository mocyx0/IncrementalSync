package org.pangolin.xuzhe.stringparser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import static org.pangolin.xuzhe.stringparser.Constants.schemaName;
import static org.pangolin.xuzhe.stringparser.Constants.tableName;

/**
 * Created by ubuntu on 17-6-7.
 */
public class LogParser {


    public static void parseToIndex(String str, int fileNo, int position, ArrayList<String> out) {
        try {
            out.clear();
            StringTokenizer tokenizer = new StringTokenizer(str, "|", false);
            while (tokenizer.hasMoreElements()) {
                out.add(tokenizer.nextToken());
            }
            if (!getDatabaseName(out).equals(schemaName) || !getTableName(out).equals(tableName)) {
                return;
            }
//            long pk = getPK(out);
////            indexes.appendIndex(pk, fileNo, position);
//            // TODO: 添加针对PK被修改，查找该PK时仍然有记录的错误处理
//            LocalLogIndex.appendIndex2(pk, fileNo, position);
//            appendIndex(out,fileNo, position);
        } catch (Exception e) {
            System.out.println("parseToIndex 解析错误" + str);
            e.printStackTrace();
        }
    }

    public static long getTimestamp(ArrayList<String> items) {
        return Long.parseLong(items.get(1));
    }

    public static String getDatabaseName(ArrayList<String> items) {
        return items.get(2);
    }

    public static String getTableName(ArrayList<String> items) {
        return items.get(3);
    }

    public static String getOpType(ArrayList<String> items) {
        return items.get(4);
    }

    public static void appendIndex(ArrayList<String> items,int fileNo, int position){
        String pk1 = null, pk2 = null;
        char opType = getOpType(items).charAt(0);
        String oldPk = getColumnAllInfoByIndex(items, 0)[1];
        String newPk = getColumnAllInfoByIndex(items, 0)[2];
        if(opType == 'D') {
            pk1 = oldPk;
        } else if(opType == 'U' && !oldPk.equals(newPk)){
            pk1 = oldPk;
            pk2 = newPk;
        } else{
            pk1 = newPk;
        }
            LocalLogIndex.appendIndex2(Long.parseLong(pk1), fileNo, position);
        if(pk2 != null)
            LocalLogIndex.appendIndex2(Long.parseLong(pk2), fileNo, position);
    }

    // TODO 暂时假设主键在日志中是第一列，后期查看canel生成日志的源码验证
//    public static long getPK(ArrayList<String> items) {
//        String pk;
//        if(getOpType(items).charAt(0) == 'D') {
//            pk = getColumnAllInfoByIndex(items, 0)[1];
//
//        } else {
//            pk = getColumnAllInfoByIndex(items, 0)[2];
//        }
//        return Long.parseLong(pk);
//    }

    public static String[] getAllColumn(ArrayList<String> items) {
        int cnt = getColumnCount(items);
        String[] result = new String[cnt];
        for(int i = 0; i < cnt; i++) {
            result[i] = items.get(5+i*3);
        }
        return result;
    }

    public static int getColumnCount(ArrayList<String> items) {
        return (items.size() - 5) / 3;
    }

    public static String[] getColumnAllInfoByIndex(ArrayList<String> items, int index) {
        String[] result = new String[3];
        result[0] = items.get(5+3*index);
        result[1] = items.get(6+3*index);
        result[2] = items.get(7+3*index);
        return result;
    }

    public static String[] getColumnMetaInfo(String str) {
        return str.split(":");
    }
}
