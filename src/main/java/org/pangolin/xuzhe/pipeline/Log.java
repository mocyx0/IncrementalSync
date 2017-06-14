package org.pangolin.xuzhe.pipeline;


import java.io.UnsupportedEncodingException;

/**
 * Created by ubuntu on 17-6-6.
 */
public class Log {
    public char op;
    public ColumnLog[] columns;

    public static void parser(byte[] bytes, int offest, int limit) {
        String str = getString(bytes, offest, limit);
        System.out.println(str);
    }

    public static Log parser(String str) {
        String line = str;
        String[] items = line.split("\\|");
        Log log = new Log();
        log.op = items[5].charAt(0);       //获取Log的操作类型
        log.columns = ColumnLog.parser(items);
        return log;
    }

    public static String getString(byte[] bytes, int offset, int limit) {
        try {
            return new String(bytes, offset, limit - offset, "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
