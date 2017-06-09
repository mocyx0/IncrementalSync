package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import org.pangolin.xuzhe.test.IOPerfTest;
import org.pangolin.xuzhe.test.ReadingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by yangxiao on 2017/6/4.
 */
public class MServer {
    private static Logger logger;

    private static void initProperties() {
        System.setProperty("middleware.test.home", Config.TESTER_HOME);
        System.setProperty("middleware.log.home", Config.LOG_HOME);
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
        logger.info(String.format("linear hashing mem: %d", LinearHashing.TOTAL_MEM.get()));
        logger.info(String.format("byte index mem: %d", LogOfTable.TOTAL_MEM.get()));
        //write to file
        if (Config.SINGLE) {
            ResultWriter.writeToFile(result);
            return null;
        } else {
            ByteBuffer re = ResultWriter.writeToBuffer(result);
            return re;
        }
    }


    private static ByteBuffer doTest() throws Exception {
        logger.info("doTest start");
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.put("hello wprld".getBytes());
        /*
        IOPerfTest.positiveOrderReadByFileChannel(Config.DATA_HOME + "/1.txt");
        // 不读同一个文件，避免从pagecache读
        IOPerfTest.reverseOrderReadByFileChannel(Config.DATA_HOME + "/2.txt");
        String[] fileNameArray = new String[10];
        for (int i = 1; i <= 10; i++) {
            fileNameArray[i - 1] = String.format("%s/%d.txt", Config.DATA_HOME, i);
        }
        ReadingThread readingThread = new ReadingThread(fileNameArray);
        readingThread.start();
        readingThread.join();
        */
        //yx test
        LogParserTest.parseLog();
        logger.info("doTest done");
        return buffer;

    }

    public static void main(String[] args) {
        String runtime = "ali";
        if (args.length >= 5) {
            runtime = args[4];
        }
        Config.init(runtime);
        initProperties();
        logger = LoggerFactory.getLogger(Server.class);
        logger.info("mserver start ");
        for (String s : args) {
            logger.info(s);
        }

        try {
            if (args.length >= 4) {
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

                ByteBuffer buffer;
                if (Config.TEST_MODE) {
                    buffer = doTest();

                } else {
                    buffer = getResult(query);
                }
                if (buffer != null) {
                    NetServerHandler.data = buffer;
                    NetServer.start();
                }
            } else {
                logger.info("参数错误");
            }
        } catch (Exception e) {
            logger.info("{}", e);
            System.exit(0);
        }
    }

}
