package org.pangolin.xuzhe.pipeline;

import org.pangolin.xuzhe.Column;

/**
 * Created by ubuntu on 17-6-6.
 */
public class ColumnLog {
    public enum Type {
        String,
        Number
    }
    public Column columnInfo;
    public String oldStringValue;
    public String newStringValue;
    public Long oldLongValue;
    public Long newLongValue;

    public ColumnLog() {

    }

    public static ColumnLog[] parser(String[] items) {
        int size = (items.length - 6) / 3;  //这个是提取出一条Log中有多少字段
        ColumnLog[] results = new ColumnLog[size];
        for(int i = 0; i < size; i++) {
            int itemIndex = 6+3*i;
            ColumnLog log = new ColumnLog();
            Column column = Column.parser(items[itemIndex]);
            log.columnInfo = column;
            if(column.type == '1') {
                if(items[itemIndex+1].charAt(0) == 'N') {
                    log.oldLongValue = null;
                } else {
                    log.oldLongValue = Long.parseLong(items[itemIndex+1]);
                }
                if(items[itemIndex+2].charAt(0) == 'N') {
                    log.newLongValue = null;
                } else {
                    log.newLongValue = Long.parseLong(items[itemIndex + 2]);
                }
            } else {
                if(items[itemIndex+1].charAt(0) == 'N') {
                    log.oldStringValue = null;
                } else {
                    log.oldStringValue = items[itemIndex+1];
                }
                if(items[itemIndex+2].charAt(0) == 'N') {
                    log.newStringValue = null;
                } else {
                    log.newStringValue = items[itemIndex+2];
                }
            }
            results[i] = log;
        }
        return results;
    }
}
