package org.pangolin.xuzhe.pipeline;

import com.koloboke.collect.set.hash.HashLongSets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import static org.pangolin.xuzhe.stringparser.Constants.schemaName;
import static org.pangolin.xuzhe.stringparser.Constants.tableName;

/**
 * Created by ubuntu on 17-6-7.
 */
public class LogParser {
    private static final Set<Long> pkSet = HashLongSets.newMutableSet(2000000);

    public static void updatePkSet(long beginPk, long endPk){
        for(long i = endPk - 1; i > beginPk ;i--){
            pkSet.add(i);
        }
    }

    public static boolean isBelongsToClient(String str,ArrayList<String> out ) {
        try {
            out.clear();
            StringTokenizer tokenizer = new StringTokenizer(str, "|", false);
            while (tokenizer.hasMoreElements()) {
                out.add(tokenizer.nextToken());
            }
            if (!getDatabaseName(out).equals(schemaName) || !getTableName(out).equals(tableName)) {
                return false;
            }
        } catch (Exception e) {
            System.out.println("parseToIndex 解析错误" + str);
            e.printStackTrace();
        }
        return judgePk(out);
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
    public static boolean judgePk(ArrayList<String> items){

        char opType = getOpType(items).charAt(0);
        String oldPk = getColumnAllInfoByIndex(items, 0)[1];
        String newPk = getColumnAllInfoByIndex(items, 0)[2];
        if(opType == 'D' && pkSet.contains(Long.parseLong(oldPk))){
            pkSet.remove(Long.parseLong(oldPk));
            return true;
        }else if(opType == 'U'){
            if(!oldPk.equals(newPk)){
                if(pkSet.contains(Long.parseLong(newPk))){
                    pkSet.remove(Long.parseLong(newPk));
                    if(!pkSet.contains(Long.parseLong(oldPk)))
                        pkSet.add(Long.parseLong(oldPk));
                    return true;
                }else{
                    if(pkSet.contains(Long.parseLong(oldPk))){
                        pkSet.remove(Long.parseLong(oldPk));
                    }
                }
            }else if(oldPk.equals(newPk) && pkSet.contains(Long.parseLong(oldPk))){
                return true;
            }
        }else if(opType == 'I' && pkSet.contains(Long.parseLong(newPk))){
                return true;
        }
//        char opType = getOpType(items).charAt(0);
//        String oldPk = getColumnAllInfoByIndex(items, 0)[1];
//        String newPk = getColumnAllInfoByIndex(items, 0)[2];
//        if(opType == 'D' &&  isInPk(Long.parseLong(oldPk),beginPk,endPk)) {
//                return true;
//        }else if(opType == 'I' && isInPk(Long.parseLong(newPk),beginPk,endPk)){
//            return true;
//        }else if(opType == 'U' && (isInPk(Long.parseLong(oldPk),beginPk,endPk) || isInPk(Long.parseLong(newPk),beginPk,endPk))){
//            return true;
//        }
        return false;
    }

//    private static boolean isInPk(long pk, long beginPk, long endPk){
//        if(pk > beginPk && pk < endPk)
//            return true;
//        return false;
//    }
    public static String[] getColumnAllInfoByIndex(ArrayList<String> items, int index) {
        String[] result = new String[3];
        result[0] = items.get(5+3*index);
        result[1] = items.get(6+3*index);
        result[2] = items.get(7+3*index);
        return result;
    }

}
