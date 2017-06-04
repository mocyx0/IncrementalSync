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
                int endId = Integer.parseInt(args[3]);
                LogParser parser = new LogParser();

                LogIndex data = parser.parseLog();

                QueryData query = new QueryData();
                query.scheme = scheme;
                query.table = table;
                query.start = startId;
                query.end = endId;

                LogRebuilder rebuider=new LogRebuilder(data);
                rebuider.getResult(query);


            } else {
                System.out.println("参数错误");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

}
