package org.pangolin.xuzhe.pipeline;

import org.pangolin.xuzhe.HashUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ubuntu on 17-6-5.
 */
public class Record {
    private Long pk; //主键值
    private Map<String, Object> values = new HashMap<>();
    private Log log;

    public Log getLog() {
        return log;
    }

    public void setLog(Log log) {

        this.log = log;
    }

    private static final List<Long> list = new ArrayList<>();
    private static Map<Integer, byte[]> columnIDMap = new HashMap<>();  // key:id, value:columnBytes
    private static Map<Integer, Integer> columnHashMap = new HashMap<>(); // key:hashCode, value:id
    private static AtomicInteger nextID = new AtomicInteger(0);

    public Record(Long pk) {
        if (pk == null) {
            throw new RuntimeException("主键值不可为null");
        }
            this.pk = pk;
        }

    public void updateColumn(String columnName, Object value) {
        values.put(columnName, value);
    }

    public Long getPk() {
        return pk;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public static List<Long> getList() {
        return list;
    }


    public static Integer getColumnID(byte[] columnName, int len) {
        int hashCode = HashUtil.hash(columnName, len);
        Integer id = columnHashMap.get(hashCode);
        if (id == null) {
            id = nextID.incrementAndGet();
            columnIDMap.put(id, Arrays.copyOf(columnName, len));
            columnHashMap.put(hashCode, id);
        }
        return id;
    }

    public String[] updateInsertInfo(Log log){
        String[] result = new String[50];
        int length = log.columns.length;
        ColumnLog columnLog = null;
        for(int i = 0; i < length; i++){
            if(i == 0){
                result[i] = String.valueOf(this.pk);
            }else{
                columnLog = log.columns[i];
                if (!values.containsKey(columnLog.columnInfo.name)) {
                    if (columnLog.columnInfo.type == '1')
                        result[i] = String.valueOf(columnLog.newLongValue);
                    else
                        result[i] = columnLog.newStringValue;
                }else{
                    result[i] = String.valueOf(values.get(columnLog.columnInfo.name));
                }
            }
        }
        return result;
    }
    public void updateResult(Log log) {
        for (ColumnLog columnLog : log.columns) {
            if (!values.containsKey(columnLog.columnInfo.name)) {
                if (columnLog.columnInfo.type == '1')
                    this.updateColumn(columnLog.columnInfo.name, columnLog.newLongValue);
                else
                    this.updateColumn(columnLog.columnInfo.name, columnLog.newStringValue);
            }
        }
    }

    public static Record createFromLastLog(Log log) {
        Record record = null;

        for (ColumnLog columnLog : log.columns) {
            if (columnLog.columnInfo.isPK) {
                record = new Record(columnLog.newLongValue);
            }
        }

        return record;
    }


}

