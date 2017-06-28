package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Client;

/**
 * Created by yangxiao on 2017/6/4.
 */
public class MClient {
    private static void initProperties() {
        System.setProperty("middleware.test.home", Config.TESTER_HOME);
        System.setProperty("middleware.teamcode", Config.TEAMCODE);
        System.setProperty("app.logging.level", Config.LOG_LEVEL);
        System.setProperty("middleware.log.home", Config.LOG_HOME);
    }


    public static void main(String[] args) {
        MLog.info("mclient start");
        MLog.info(String.format("time %d", System.currentTimeMillis()));
        /*
        logger.info("args: ");
        for (String s : args) {
            logger.info(s);
        }
        */
        Config.init();
        try {
            initProperties();
            NetClient.start(args[0]);
        } catch (Exception e) {
            MLog.info(e);
            System.exit(0);
        }
    }
}
