package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Constants;
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
    private static Logger logger = LoggerFactory.getLogger(MServer.class);

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
        //write to file
        if (Config.SINGLE) {
            ResultWriter.writeToFile(result);
            return null;
        } else {
            ByteBuffer re = ResultWriter.writeToBuffer(result);
            return re;
        }

    }

    private static ByteBuffer doTest() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.put("hello wprld".getBytes());
        try {
            IOPerfTest.positiveOrderReadByFileChannel(Constants.DATA_HOME + "/1.txt");
            // 不读同一个文件，避免从pagecache读
            IOPerfTest.reverseOrderReadByFileChannel(Constants.DATA_HOME + "/2.txt");
            String[] fileNameArray = new String[10];
            for(int i = 1; i <= 10; i++) {
                fileNameArray[i-1] = String.format("%s/%d.txt", Constants.DATA_HOME, i);
            }
            ReadingThread readingThread = new ReadingThread(fileNameArray);
            readingThread.start();
            readingThread.join();
        } catch (IOException e) {
            logger.info("{}", e);
        } catch (InterruptedException e) {

        }



        return buffer;
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
                System.out.println("参数错误");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

}
