package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) {
        logger.info("mclient start");
        logger.info("args: ");
        for (String s : args) {
            logger.info(s);
        }
        String runtime = "ali";
        if (args.length >= 2) {
            runtime = args[1];
        }
        Config.init(runtime);


        try {
            initProperties();
            NetClient.start(args[0]);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}
