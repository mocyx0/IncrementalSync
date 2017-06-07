package org.pangolin.yx;

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

        try {
            initProperties();
            NetClient.start(args[0]);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}
