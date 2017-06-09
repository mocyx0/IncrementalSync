package org.pangolin.yx;

import com.alibaba.middleware.race.sync.*;

/**
 * Created by yangxiao on 2017/6/4.
 */
public class Config {
    public static void init(String runtime) {
        //使用阿里环境的配置
        if (runtime.equals("ali")) {
            TESTER_HOME = Constants.TESTER_HOME;
            DATA_HOME = Constants.DATA_HOME;
            RESULT_HOME = Constants.RESULT_HOME;
            TEAMCODE = Constants.TEAMCODE;
            LOG_LEVEL = Constants.LOG_LEVEL;
            MIDDLE_HOME = Constants.MIDDLE_HOME;
            SERVER_PORT = Constants.SERVER_PORT;
            LOG_HOME = Constants.LOG_HOME;
        } else if (runtime.equals("yx")) {

        } else {
            String rt = System.getenv("RUNTIME");
            if (rt != null && runtime.equals("xuzhe")) {
                TESTER_HOME = "/home/ubuntu/alitest/";
                DATA_HOME = "/home/ubuntu/alitest/data";
                RESULT_HOME = "/home/ubuntu/alitest/result";
                TEAMCODE = Constants.TEAMCODE;
                LOG_LEVEL = "DEBUG";
                MIDDLE_HOME = "/home/ubuntu/alitest/middle";
                SERVER_PORT = Constants.SERVER_PORT;
                LOG_HOME = "/home/ubuntu/alitest/log";
            }
        }
    }

    // 工作主目录
    public static String TESTER_HOME = "D:/tmp/testhome";
    // 赛题数据
    public static String DATA_HOME = "D:/tmp/amimid/log";
    // 结果文件目录
    public static String RESULT_HOME = "D:/tmp/amimid/result";
    public static String RESULT_NAME = "Result.rs";
    // teamCode
    public static String TEAMCODE = "yx";
    // 日志级别
    public static String LOG_LEVEL = "INFO";
    // 中间结果目录
    public static String MIDDLE_HOME = "D:/tmp/amimid/middle";
    //
    public static String LOG_HOME = "D:/tmp/testhome/logs/" + TEAMCODE;
    // server端口
    public static Integer SERVER_PORT = 5527;

    public static int BLOCK_SIZE = 1024 * 1024 * 16;

    public static int TYPE_NUMBER = 1;
    public static int TYPE_STRING = 2;

    //单机模式
    public static boolean SINGLE = false;
    public static QueryData queryData;
    //test mode会执行mserver的doTest并且只会返回client "hello world"
    public static boolean TEST_MODE = true;

    public static byte OP_TYPE_DELETE = 1;
    public static byte OP_TYPE_INSERT = 2;
    public static byte OP_TYPE_UPDATE = 3;
}
