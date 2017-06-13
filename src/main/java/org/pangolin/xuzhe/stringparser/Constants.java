package org.pangolin.xuzhe.stringparser;

import org.pangolin.yx.Config;

/**
 * Created by ubuntu on 17-6-3.
 */
public class Constants {
    static final int BUFFER_SIZE = 1<<20; // 1M
    static final int POOL_SIZE = 180;
    static final int WORKER_NUM = 1;
    static final int LINE_MAX_LENGTH = 2000;
    public static String schemaName = "middleware3";
    public static String tableName = "student";
    public static String getFileNameByNo(int no) {
        return String.format("%s/splited.txt%02d", Config.DATA_HOME, no);
    }
}
