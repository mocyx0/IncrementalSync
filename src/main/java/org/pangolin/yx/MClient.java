package org.pangolin.yx;

/**
 * Created by yangxiao on 2017/6/4.
 */
public class MClient {
    private static void initProperties() {
        System.setProperty("middleware.test.home", Config.TESTER_HOME);
        System.setProperty("middleware.teamcode", Config.TEAMCODE);
        System.setProperty("app.logging.level", Config.LOG_LEVEL);
    }

    public static void main(String[] args) {

        try {
            initProperties();
            NetClient.start("127.0.0.1");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}
