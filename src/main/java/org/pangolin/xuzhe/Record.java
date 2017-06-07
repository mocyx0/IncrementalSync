package org.pangolin.xuzhe;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by ubuntu on 17-6-5.
 */
public class Record {
    private Long pk; //主键值
    private Map<String, Object> values = new HashMap<>();
    private boolean finished = false;;
    private boolean deleted = false;
    private static Map<Integer, byte[]> columnIDMap = new HashMap<>();  // key:id, value:columnBytes
    private static Map<Integer, Integer> columnHashMap = new HashMap<>(); // key:hashCode, value:id
    private static AtomicInteger nextID = new AtomicInteger(0);
    public Record(Long pk) {
        if(pk == null) {
            throw new RuntimeException("主键值不可为null");
        }
        this.pk = pk;
    }

    public void updateColumn(String columnName, Object value) {
        values.put(columnName, value);
    }

    public static Integer getColumnID(byte[] columnName, int len) {
        int hashCode = HashUtil.hash(columnName, len);
        Integer id = columnHashMap.get(hashCode);
        if(id == null) {
            id = nextID.incrementAndGet();
            columnIDMap.put(id, Arrays.copyOf(columnName, len));
            columnHashMap.put(hashCode, id);
        }
        return id;
    }

    public void update(Log log) {
        if(log.op == 'I') {
            for(ColumnLog columnLog : log.columns) {
                if(columnLog.columnInfo.type == '1')
                    this.updateColumn(columnLog.columnInfo.name ,columnLog.newLongValue);
                else
                    this.updateColumn(columnLog.columnInfo.name ,columnLog.newStringValue);
            }
            this.finished = true;
        } else if(log.op == 'U') {
            for(ColumnLog columnLog : log.columns) {
                if(columnLog.columnInfo.type == '1')
                    this.updateColumn(columnLog.columnInfo.name ,columnLog.newLongValue);
                else
                    this.updateColumn(columnLog.columnInfo.name ,columnLog.newStringValue);
            }
            this.finished = false;
        } else if(log.op == 'D') {
            this.deleted = true;
            this.finished = true;
        } else {
            throw new RuntimeException("Error,op值非法: "+log.op);
        }
    }

    public static Record createFromLastLog(Log log) {
        Record record = null;
        for(ColumnLog columnLog : log.columns) {
            if(columnLog.columnInfo.isPK) {
                record = new Record(columnLog.newLongValue);
            }
        }
        record.update(log);

        return record;
    }

    public static void main(String[] args) {
        String[] logTexts = {
                "|mysql-bin.00001717148759|1496736165000|middleware3|student|I|id:1:1|NULL|1|first_name:2:0|NULL|徐|last_name:2:0|NULL|依|sex:2:0|NULL|男|score:1:0|NULL|66|",
                "|mysql-bin.000017118746255|1496737772000|middleware3|student|U|id:1:1|1|1|score:1:0|66|835|",
                "|mysql-bin.000017290363889|1496737797000|middleware3|student|U|id:1:1|1|1|first_name:2:0|徐|周|",
                "|mysql-bin.000017335560991|1496737801000|middleware3|student|U|id:1:1|1|1|score:1:0|835|999|\n",
                "|mysql-bin.000017442746557|1496737815000|middleware3|student|U|id:1:1|1|1|first_name:2:0|周|林|\n",
                "|mysql-bin.000017487943659|1496737820000|middleware3|student|U|id:1:1|1|1|score:1:0|999|247|"
        };

        Record r = Record.createFromLastLog(Log.parse(logTexts[5]));
        r.update(Log.parse(logTexts[4]));
        r.update(Log.parse(logTexts[3]));
        r.update(Log.parse(logTexts[2]));
        r.update(Log.parse(logTexts[1]));
        r.update(Log.parse(logTexts[0]));
        System.out.println(r);

    }
}
