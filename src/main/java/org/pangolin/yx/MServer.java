package org.pangolin.yx;

import java.util.List;

/**
 * Created by yangxiao on 2017/6/4.
 */
public class MServer {
    public static void main(String[] args) {

        try {
            Config.setRuntime("yx");
            if (args.length == 4) {
                String scheme = args[0];
                String table = args[1];
                int startId = Integer.parseInt(args[2]);
                int end = Integer.parseInt(args[3]);
                LogParser parser = new LogParser();
                parser.parseLog();
                List<Record> data = parser.getResult();

            } else {
                System.out.println("参数错误");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

}
