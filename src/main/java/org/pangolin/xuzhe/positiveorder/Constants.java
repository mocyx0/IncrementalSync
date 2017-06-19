package org.pangolin.xuzhe.positiveorder;

import org.pangolin.yx.Config;

/**
 * Created by ubuntu on 17-6-3.
 */
public class Constants {

    static final int REDO_NUM = 3;
    static final int BUFFER_SIZE = 128*1024;
    static final int PARSER_NUM = 3;
    static final int READBUFFER_POOL_SIZE = 10; // 至少2个
    static final int LINE_MAX_LENGTH = 1000;
    static final int STRING_LIST_SIZE = 5000;
    static final int PARSER_BLOCKING_QUEUE_SIZE = READBUFFER_POOL_SIZE;
    static final int LOGINDEXPOOL_SIZE = READBUFFER_POOL_SIZE; // POOL中的实例个数
    static final int LOGINDEX_SIZE = 3000; // 每个LogIndex实例中能够保存的最大Log个数
    public static String schemaName = "middleware3";
    public static String tableName = "student";
    public static String getFileNameByNo(int no) {
        return String.format("%s/splited.txt%02d", Config.DATA_HOME, no);
    }
}
