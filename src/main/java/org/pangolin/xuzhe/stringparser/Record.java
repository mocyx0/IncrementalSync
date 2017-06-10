package org.pangolin.xuzhe.stringparser;

import org.pangolin.xuzhe.HashUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.pangolin.xuzhe.stringparser.ReadingThread.getLineByPosition;

/**
 * Created by ubuntu on 17-6-5.
 */
public class Record {
    private Long pk; //主键值
    private Map<String, Object> values = new HashMap<>();
    private static final List<Long> list = new ArrayList<>();
    private boolean finished = false;
    private boolean deleted = false;
    private int fileNo;
    private long position;
    private static Map<Integer, byte[]> columnIDMap = new HashMap<>();  // key:id, value:columnBytes
    private static Map<Integer, Integer> columnHashMap = new HashMap<>(); // key:hashCode, value:id
    private static AtomicInteger nextID = new AtomicInteger(0);

    public Record(Long pk, int fileNo, long position) {
        if (pk == null) {
            throw new RuntimeException("主键值不可为null");
        }
            this.pk = pk;
            this.fileNo = fileNo;
            this.position = position;
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

    //    public void update(Log log, LocalLogIndex indexes) {
//        if(log.op == 'I') {
//            insertResult(log);
//        } else if(log.op == 'U') {
//            updateResult(log, indexes);
//        } else if(log.op == 'D') {
//            this.deleted = true;
//            this.finished = true;
//        } else {
//            throw new RuntimeException("Error,op值非法: "+log.op);
//        }
//    }
//
//    private void insertResult(Log log){
//        for(ColumnLog columnLog : log.columns) {
//            if(!values.containsKey(columnLog.columnInfo.name)){
//                if(columnLog.columnInfo.type == '1')
//                    this.updateColumn(columnLog.columnInfo.name ,columnLog.newLongValue);
//                else
//                    this.updateColumn(columnLog.columnInfo.name ,columnLog.newStringValue);
//            }
//        }
//    }
//    private void updateResult(Log log, LocalLogIndex indexes){
//        long oldKey = 0, newKey = 0;
//        for(ColumnLog columnLog : log.columns) {
//            if(columnLog.columnInfo.isPK) {
//                oldKey = columnLog.oldLongValue;
//                newKey = columnLog.newLongValue;
//            }
//            if(!values.containsKey(columnLog.columnInfo.name)){
//                if(columnLog.columnInfo.type == '1')
//                    this.updateColumn(columnLog.columnInfo.name ,columnLog.newLongValue);
//                else
//                    this.updateColumn(columnLog.columnInfo.name ,columnLog.newStringValue);
//            }
//        }
//
//        if(oldKey != newKey && newKey != pk){
//            List<LocalLogIndex.IndexEntry> logs = indexes.indexes.get(oldKey);
//            Collections.sort(logs);
//            Collections.reverse(logs);
//            for(LocalLogIndex.IndexEntry index : logs) {
//                String line = getLineByPosition(index.fileNo, index.position);
//                Log log1 = Log.parser(line);
//                    //如果是插入操作就到此终止
//                    this.update(log1, indexes);
//                    if(log1.op == 'I'){
//                        break;
//                    }
//            }
//        }
//    }
//    public static Record createFromLastLog(Log log, LocalLogIndex indexes) {
//        Record record = null;
//        for(ColumnLog columnLog : log.columns) {
//            if(columnLog.columnInfo.isPK) {
//                record = new Record(columnLog.newLongValue);
//            }
//        }
//        record.update(log,indexes);
//        return record;
//    }


    public void insertResult(Log log) {
        for (ColumnLog columnLog : log.columns) {
            if (!values.containsKey(columnLog.columnInfo.name)) {
                if (columnLog.columnInfo.type == '1')
                    this.updateColumn(columnLog.columnInfo.name, columnLog.newLongValue);
                else
                    this.updateColumn(columnLog.columnInfo.name, columnLog.newStringValue);
            }
        }
    }

    public void setFileNo(int fileNo) {
        this.fileNo = fileNo;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public int getFileNo() {

        return fileNo;
    }

    public long getPosition() {
        return position;
    }

    public static Record createFromLastLog(Log log, LocalLogIndex indexes, int fileNo, long position) {
        Record record = null;
        for (ColumnLog columnLog : log.columns) {
            if (columnLog.columnInfo.isPK) {
                record = new Record(columnLog.newLongValue, fileNo, position);
            }
        }
 //       record.update(log, indexes, fileNo, position);
        return record;
    }

    public static List<Long> getList() {
        return list;
    }
}

