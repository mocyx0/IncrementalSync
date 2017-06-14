package org.pangolin.xuzhe.pipeline;

import java.util.*;


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

    //对values根据insert的log进行重新排序
    public String[] updateInsertInfo(Log log) {
        String[] result = new String[values.size()];
        int length = log.columns.length;
        ColumnLog columnLog = null;
        for (int i = 0; i < length; i++) {
            if (i == 0) {
                result[i] = String.valueOf(this.pk);
            } else {
                columnLog = log.columns[i];
                if (!values.containsKey(columnLog.columnInfo.name)) {
                    if (columnLog.columnInfo.type == '1')
                        result[i] = String.valueOf(columnLog.newLongValue);
                    else
                        result[i] = columnLog.newStringValue;
                } else {
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

}

