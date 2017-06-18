package org.pangolin.yx;

import com.alibaba.middleware.race.sync.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangxiao on 2017/6/4.
 */


public class Config {
    private static String runtime = "yx";

    public static void init() {
        String runtime = System.getenv("RUNTIME");
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

            BLOCK_SIZE = Constants.BLOCK_SIZE;

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
        } else if (runtime.equals("zsn")) {
            TESTER_HOME = "G:/研究生/AliCompetition/quarter-final/home";
            DATA_HOME = "G:/研究生/AliCompetition/quarter-final/home/data";
            RESULT_HOME = "G:/研究生/AliCompetition/quarter-final/home/result";
            TEAMCODE = Constants.TEAMCODE;
            LOG_LEVEL = "DEBUG";
            MIDDLE_HOME = "G:/研究生/AliCompetition/quarter-final/home/middle";
            SERVER_PORT = Constants.SERVER_PORT;
            LOG_HOME = "G:/研究生/AliCompetition/quarter-final/home/log";
        }
    }

    // 工作主目录
    public static String TESTER_HOME = "D:/tmp/testhome";
    // 赛题数据
    public static String DATA_HOME = "C:/tmp/logfinal";
    // 结果文件目录
    public static String RESULT_HOME = "D:/tmp/amimid/result";
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
    public static Integer SERVER_PORT = 5527;

    public static long BLOCK_SIZE = 1024 * 1024 * 256;

    public static int TYPE_NUMBER = 1;
    public static int TYPE_STRING = 2;

    //客户端打印一部分输出
    public static int PRINT_RESULT_LINE = 50;

    public static QueryData queryData;
    //test mode会执行mserver的doTest并且只会返回client "hello world"
    //real test mix
    //mix会一起执行real和test
    public static String TEST_MODE = "real";

    public static byte OP_TYPE_DELETE = 1;
    public static byte OP_TYPE_INSERT = 2;
    public static byte OP_TYPE_UPDATE = 3;

    public static boolean COPY_DATA = false;

    public static boolean NOT_CHECK_SCHEME = false;

    //precache开始后多久开始执行日志分析
    public static int PRECACHE_DELAY = 3000;
    public static int PRECACHE_THREAD = 4;
    public static int CPU_COUNT = 4;
    public static int REBUILDER_THREAD = 2;
    public static int COLLECTOR_THREAD = 4;
    public static int PARSER_THREAD = 2;

    public static int MAX_COL_SIZE = 6;
    //
    public static Logger serverLogger = LoggerFactory.getLogger(Server.class);

}

