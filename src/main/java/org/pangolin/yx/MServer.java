package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import io.netty.channel.Channel;
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
        //precache
        //PreCache.precache(Util.logFiles(Config.DATA_HOME));

        //等待precache一部分数据
        //Thread.sleep(Config.PRECACHE_DELAY);
        /*
        synchronized (PreCache.class) {
            PreCache.class.wait();
        }
        */

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

        //yx test
        //LogParserTest.parseLog();

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

        logger.info("doTest done");
        return buffer;

    }


    private static class Worker implements Runnable {

        @Override
        public void run() {
            try {
                //运行我们的程序
                ByteBuffer buffer;
                if (Config.TEST_MODE.equals("test")) {
                    buffer = doTest();
                } else if (Config.TEST_MODE.equals("real")) {
                    buffer = getResult(Config.queryData);
                } else if (Config.TEST_MODE.equals("mix")) {
                    doTest();
                    buffer = getResult(Config.queryData);
                } else {
                    throw new Exception("wrong test mode");
                }
                logger.info("send result to client");
                if (buffer != null) {
                    //发送结果
                    NetServerHandler.sendResult(buffer);
                }
            } catch (Exception e) {
                logger.info("{}", e);
                System.exit(0);
            } catch (Error e) {
                logger.info("{}", e);
                throw e;
            }
        }
    }

    public static void main(String[] args) {
        Config.init();
        initProperties();
        logger = LoggerFactory.getLogger(Server.class);
        logger.info("mserver start ");
        logger.info("args:  ");

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

                //开启网络服务
                //NetServerHandler.data = buffer;
                Thread th = new Thread(new Worker());
                th.start();
                NetServer.start();
            } else {
                logger.info("参数错误");
            }
        } catch (Exception e) {
            logger.info("{}", e);
            System.exit(0);
        } catch (Error e) {
            logger.info("{}", e);
            logger.info(e.toString());
            throw e;
        }
    }

}
