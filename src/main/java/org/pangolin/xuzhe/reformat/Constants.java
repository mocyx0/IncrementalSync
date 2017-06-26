package org.pangolin.xuzhe.reformat;

/**
 * Created by ubuntu on 17-6-3.
 */
public class Constants {

    static final int REDO_NUM = 3;
    static final int BUFFER_SIZE = 512*1024;
    static final int PARSER_NUM = 7;
    static final int UNCOMP_NUM = 9;
    static final int READBUFFER_POOL_SIZE = 20; // 至少2个
    static final int LINE_MAX_LENGTH = 1000;
    static final int BYTEARRAY_SIZE = 512*1024;
    static final int STRING_LIST_SIZE = 5000;
    static final int PARSER_BLOCKING_QUEUE_SIZE = READBUFFER_POOL_SIZE/2;
    static final int BYTEARRAY_POOL_SIZE = PARSER_BLOCKING_QUEUE_SIZE*PARSER_NUM;
    static final int LOGINDEXPOOL_SIZE = READBUFFER_POOL_SIZE; // POOL中的实例个数
    static final int LOGINDEX_SIZE = 6000; // 每个LogIndex实例中能够保存的最大Log个数
}