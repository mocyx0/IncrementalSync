package org.pangolin.xuzhe.pipeline;

import org.pangolin.xuzhe.Column;

/**
 * Created by ubuntu on 17-6-6.
 */
public class Schema {
    private String tableName;
    private Column[] columns;
    private Schema() {}
    public static Schema generateFromInsertLog(String str) {
        String[] items = str.split("|");

        for(String item : items) {
            System.out.println(items);
        }
        Schema s = new Schema();
        s.tableName = items[2];
        return s;

    }

}
