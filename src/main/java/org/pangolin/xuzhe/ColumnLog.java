package org.pangolin.xuzhe;

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
        int size = (items.length - 6) / 3;
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

//    public int parse(byte[] bytes,int offset, int limit) {
//        try {
//            String str = new String(bytes, offset, limit, "utf-8");
//            String[] items = str.split("|");
//        } catch (UnsupportedEncodingException e) {
//
//        }
//        int next = offset,  i = offset, j;
////        i = findNext(bytes, next, limit);
//        j = i;
//        // 找到:的坐标，用于抽取出列名，类型，是否主键
//        while (bytes[j] != ':') ++j;
//        name = getString(bytes, i, j);
//        ++j;
//        if(bytes[j] == '1') type = Type.Number;
//        else type = Type.String;
//        j += 2;
//        if(bytes[j] == '0') isPK = false;
//        else isPK = true;
//        i = j+2;
//        j = i;
//        if(type == Type.Number) {
//            while(bytes[j] != '|') ++j;
//            String longStr = getString(bytes, i, j);
//            if(longStr.equals("NULL")) oldLongValue = null;
//            else oldLongValue = Long.parseLong(longStr);
//            i = j+1;
//            j = i;
//            while(bytes[j] != '|') ++j;
//            longStr = getString(bytes, i, j);
//            if(longStr.equals("NULL")) newLongValue = null;
//            else newLongValue = Long.parseLong(longStr);
//        } else {
//            while(bytes[j] != '|') ++j;
//            String str = getString(bytes, i, j);
//            if(str.equals("NULL")) oldStringValue = null;
//            else oldStringValue = str;
//            i = j+1;
//            j = i;
//            while(bytes[j] != '|') ++j;
//            str = getString(bytes, i, j);
//            if(str.equals("NULL")) newStringValue = null;
//            else newStringValue = str;
//        }
//        return j;
//    }
//    @Override
//    public String toString() {
//        StringBuilder sb = new StringBuilder();
//        sb.append(name);
//        sb.append(':');
//        if(type==Type.Number) {
//            sb.append('1');
//            sb.append(':');
//            sb.append(isPK?'1':'0');
//            sb.append('|');
//            if(oldLongValue == null) {
//                sb.append("NULL");
//            } else {
//                sb.append(oldLongValue);
//            }
//            sb.append('|');
//            if(newLongValue == null) {
//                sb.append("NULL");
//            } else {
//                sb.append(newLongValue);
//            }
//        } else {
//            sb.append('2');
//            sb.append(':');
//            sb.append(isPK?'1':'0');
//            sb.append('|');
//            if(oldStringValue == null) {
//                sb.append("NULL");
//            } else {
//                sb.append(oldStringValue);
//            }
//            sb.append('|');
//            if(newStringValue == null) {
//                sb.append("NULL");
//            } else {
//                sb.append(newStringValue);
//            }
//        }
//
//        return sb.toString();
//    }
//    public static boolean hasNextColumnLog(byte[] bytes, int offset, int limit) {
//        if(limit - offset < 2) return false;
//        if(bytes[offset] != '|') return true;
//        else if(bytes[offset+1] != '\n') {
//            return true;
//        } else {
//            return false;
//        }
//    }
//
//    public static void main(String[] args) throws UnsupportedEncodingException {
//        long begin = System.currentTimeMillis();
////        int it = 1000000;
//        int it = 1;
//        byte[] data1 = "id:1:1|NULL|102|".getBytes("utf-8");
//        byte[] data2 = "name:2:0|NULL|ljh|".getBytes("utf-8");
//        byte[] data3 = "score:1:0|NULL|98|".getBytes("utf-8");
//        for(int i = 0; i < it; ++i) {
//            ColumnLog log = new ColumnLog();
//            log.parse(data1, 0, data1.length);
//        System.out.println(log);
//            log = new ColumnLog();
//            log.parse(data2, 0, data2.length);
//        System.out.println(log);
//            log = new ColumnLog();
//            log.parse(data3, 0, data3.length);
//        System.out.println(log);
//        }
//        long end = System.currentTimeMillis();
//        System.out.println((end-begin));
//    }
}
