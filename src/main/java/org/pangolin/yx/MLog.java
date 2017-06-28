package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Constants;

import java.io.File;
import java.io.RandomAccessFile;

/**
 * Created by yangxiao on 2017/6/27.
 */
public class MLog {

    private static RandomAccessFile raf;

    public static void init(String filePath) {
        if (Constants.DO_LOG) {
            try {
                File f = new File(filePath);
                if (f.exists()) {
                    f.delete();
                }
                raf = new RandomAccessFile(filePath, "rw");
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }

    }

    public static synchronized void info(Exception s) {
        if (Constants.DO_LOG) {
            try {
                long t1 = System.currentTimeMillis();
                StringBuilder sb = new StringBuilder();
                sb.append(t1 / 1000);
                sb.append(" ");
                sb.append(t1 % 1000);
                sb.append(": ");
                sb.append(s.toString() + "\n");
                StackTraceElement[] trace = s.getStackTrace();
                for (StackTraceElement stack : trace) {
                    sb.append(stack.toString());
                    sb.append("\n");
                }
                System.out.println(sb.toString());
                if (raf != null) {
                    sb.append("\n");
                    raf.write(sb.toString().getBytes());
                }
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }

    public static synchronized void info(String s) {
        if (Constants.DO_LOG) {
            try {
                long t1 = System.currentTimeMillis();
                StringBuilder sb = new StringBuilder();
                sb.append(t1 / 1000);
                sb.append(" ");
                sb.append(t1 % 1000);
                sb.append(": ");
                sb.append(s);
                System.out.println(sb.toString());
                if (raf != null) {
                    sb.append("\n");
                    raf.write(sb.toString().getBytes());
                }
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }

}
