package org.pangolin.yx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by yangxiao on 2017/6/4.
 */
public class MServer {
    private static Logger logger = LoggerFactory.getLogger(MServer.class);

    private static void initProperties() {
        System.setProperty("middleware.test.home", Config.TESTER_HOME);
        System.setProperty("middleware.teamcode", Config.TEAMCODE);
        System.setProperty("app.logging.level", Config.LOG_LEVEL);
    }


    private static ByteBuffer getResult(QueryData query) throws Exception {

        LogParser parser = new LogParser();
        //read log
        AliLogData data = parser.parseLog();
        logger.info("parseLog done");
        //get log info
        LogRebuilder rebuider = new LogRebuilder(data);
        logger.info("rebuild done");
        //rebuild data
        RebuildResult result = rebuider.getResult();
        logger.info("getResult done");
        //write to file
        if (Config.SINGLE) {
            ResultWriter.writeToFile(result);
            return null;
        } else {
            ByteBuffer re = ResultWriter.writeToBuffer(result);
            return re;
        }

    }


    public static void main(String[] args) {
        initProperties();
        try {
            Config.setRuntime("yx");

            if (args.length == 4) {
                String scheme = args[0];
                String table = args[1];
                int startId = Integer.parseInt(args[2]);
                int endId = Integer.parseInt(args[3]);
                //build query
                QueryData query = new QueryData();
                query.scheme = scheme;
                query.table = table;
                query.start = startId;
                query.end = endId;
                Config.queryData = query;
                ByteBuffer buffer = getResult(query);
                if (buffer != null) {
                    //buffer.put("hello hello".getBytes());
                    NetServerHandler.data = buffer;
                    NetServer.start();
                }


            } else {
                System.out.println("参数错误");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

}
