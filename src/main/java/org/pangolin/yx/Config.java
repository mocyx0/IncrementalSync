package org.pangolin.yx;

import com.alibaba.middleware.race.sync.*;

import java.util.concurrent.TransferQueue;

/**
 * Created by yangxiao on 2017/6/4.
 */


public final class Config {
    private static String runtime = "yx";

    public static void init() {
        String runtime = System.getenv("RUNTIME");
        System.out.println(runtime);
        if (runtime == null) {
            //使用阿里环境的配置
            TESTER_HOME = Constants.TESTER_HOME;
            DATA_HOME = Constants.DATA_HOME;
            RESULT_HOME = Constants.RESULT_HOME;
            TEAMCODE = Constants.TEAMCODE;
            LOG_LEVEL = Constants.LOG_LEVEL;
            MIDDLE_HOME = Constants.MIDDLE_HOME;
            SERVER_PORT = Constants.SERVER_PORT;
            LOG_HOME = Constants.LOG_HOME;
            //
            CPU_COUNT = Constants.CPU_COUNT;
            PRECACHE_DELAY = Constants.PRECACHE_DELAY;
            PRECACHE_THREAD = Constants.PRECACHE_THREAD;


            COPY_DATA = Constants.COPY_DATA;

            NOT_CHECK_SCHEME = Constants.NOT_CHECK_SCHEME;

            REBUILDER_THREAD = Constants.REBUILDER_THREAD;
            COLLECTOR_THREAD = Constants.COLLECTOR_THREAD;
            PARSER_THREAD = Constants.PARSER_THREAD;


        } else if (runtime.equals("xuzhe")) {
            TESTER_HOME = "/home/ubuntu/alitest/";
            DATA_HOME = "/home/ubuntu/alitest/data/ram";
            RESULT_HOME = "/home/ubuntu/alitest/result";
            TEAMCODE = Constants.TEAMCODE;
            LOG_LEVEL = "DEBUG";
            MIDDLE_HOME = "/home/ubuntu/alitest/middle";
            SERVER_PORT = Constants.SERVER_PORT;
            LOG_HOME = "/home/ubuntu/alitest/log";
        } else if (runtime.equals("xuzhe_win10")) {
            TESTER_HOME = "D:\\Code\\java\\中间件复赛\\test_data\\1G_S10\\";
            DATA_HOME = "D:\\Code\\java\\中间件复赛\\test_data\\1G_S10\\";
            RESULT_HOME = "D:\\Code\\java\\中间件复赛\\test_data\\1G_S10\\";
            TEAMCODE = Constants.TEAMCODE;
            LOG_LEVEL = "DEBUG";
            MIDDLE_HOME = "D:\\Code\\java\\中间件复赛\\test_data\\1G_S10\\";
            SERVER_PORT = Constants.SERVER_PORT;
            LOG_HOME = "D:\\Code\\java\\中间件复赛\\test_data\\";
        } else if (runtime.equals("zsn")) {
            TESTER_HOME = "G:/研究生/AliCompetition/quarter-final/home";
            DATA_HOME = "G:/研究生/AliCompetition/quarter-final/home/data";
            RESULT_HOME = "G:/研究生/AliCompetition/quarter-final/home/result";
            TEAMCODE = Constants.TEAMCODE;
            LOG_LEVEL = "DEBUG";
            MIDDLE_HOME = "G:/研究生/AliCompetition/quarter-final/home/middle";
            SERVER_PORT = Constants.SERVER_PORT;
            LOG_HOME = "G:/研究生/AliCompetition/quarter-final/home/log";
        } else if (runtime.equals("remote_yx")) {
            TESTER_HOME = "/root/yangxiao/sync";
            DATA_HOME = "/root/ram";
            RESULT_HOME = "/root/yangxiao/result";
            MIDDLE_HOME = "/root/yangxiao/result";

            CPU_COUNT = Constants.CPU_COUNT;
            REBUILDER_THREAD = Constants.REBUILDER_THREAD;
            COLLECTOR_THREAD = Constants.COLLECTOR_THREAD;
            PARSER_THREAD = Constants.PARSER_THREAD;
        }
    }

    // 工作主目录
    public static String TESTER_HOME = "C:/tmp/alimid/small";
    // 赛题数据
    public static String DATA_HOME = "C:/tmp/alimid/big";
    // 结果文件目录
    public static String RESULT_HOME = "C:/tmp/alimid/big";
    public static String RESULT_NAME = "Result.rs";
    // teamCode
    public static String TEAMCODE = "yx";
    // 日志级别
    public static String LOG_LEVEL = "INFO";
    // 中间结果目录
    public static String MIDDLE_HOME = "C:/tmp/log10g1";
    //
    public static String LOG_HOME = "D:/tmp/testhome/logs/" + TEAMCODE;
    // server端口
    public static Integer SERVER_PORT = 5528;

    public static final long BLOCK_SIZE = 1024 * 1024 * 256;

    public static final int TYPE_NUMBER = 1;
    public static final int TYPE_STRING = 2;

    //客户端打印一部分输出//
    public static int PRINT_RESULT_LINE = 3;

    public static QueryData queryData;
    //test mode会执行mserver的doTest并且只会返回client "hello world"
    //real test mix
    //mix会一起执行real和test
    public static final String TEST_MODE = "real";

    public static final byte OP_TYPE_DELETE = 1;
    public static final byte OP_TYPE_INSERT = 2;
    public static final byte OP_TYPE_UPDATE = 3;

    public static boolean COPY_DATA = false;

    public static boolean NOT_CHECK_SCHEME = false;

    //precache开始后多久开始执行日志分析
    public static int PRECACHE_DELAY = 3000;
    public static int PRECACHE_THREAD = 4;
    public static int CPU_COUNT = 4;
    public static int REBUILDER_THREAD = 1;
    public static int COLLECTOR_THREAD = 1;
    public static int PARSER_THREAD = 1;

    public static final int MAX_COL_SIZE = 6;
    //
    public static final int PARSER_IN_QUEUE = 2;
    public static final int PARSER_OUT_QUEUE = 2;
    public static final int REBUILDER_IN_QUEUE = 128;
    public static final int READ_POOL_SIZE = 9;
    public static final int LOG_BLOCK_QUEUE = 50;
    public static final int READ_BUFFER_SIZE = (int) (1024 * 1024 * 2);

    public static final boolean OPTIMIZE = true;
    public static final int ALI_ID_MIN = 1000000;
    public static final int ALI_ID_MAX = 8000000;
}

