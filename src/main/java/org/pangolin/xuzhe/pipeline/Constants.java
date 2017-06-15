package org.pangolin.xuzhe.pipeline;

import org.pangolin.yx.Config;

/**
 * Created by ubuntu on 17-6-3.
 */
public class Constants {
    static final int BUFFER_SIZE = 128*1024; // 1M
    static final int POOL_SIZE = 4;
    static final int WORKER_NUM = 1;
    static final int LINE_MAX_LENGTH = 1000;
    static final int STRING_LIST_SIZE = 5000;
    static final int BLOCKING_QUEUE_SIZE = 50;

    public static String schemaName = "middleware3";
    public static String tableName = "student";
    public static String getFileNameByNo(int no) {
        return String.format("%s/splited.txt%02d", Config.DATA_HOME, no);
    }
}
