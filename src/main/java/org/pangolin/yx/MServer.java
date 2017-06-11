package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.pangolin.xuzhe.test.IOPerfTest;
import org.pangolin.xuzhe.test.ReadingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
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


    private static void getResult() throws Exception {
        long t1 = System.currentTimeMillis();

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
        // Thread.sleep(2000);
        //read log
        AliLogData data = parser.parseLog();
        logger.info("parseLog done");
        //rebuild
        /*
        LogRebuilder rebuider = new LogRebuilder(data);
        RebuildResult result = rebuider.getResult();
        */


        long t2 = System.currentTimeMillis();
        LogRebuilderLarge.init(data);
        LogRebuilderLarge.run();
        long t3 = System.currentTimeMillis();
        logger.info("rebuild done");
        logger.info(String.format("cost time  index:%d   rebuild:%d", t2 - t1, t3 - t2));
        logger.info(String.format("linear hashing mem: %d", LinearHashing.TOTAL_MEM.get()));
        logger.info(String.format("byte index mem: %d", LogOfTable.TOTAL_MEM.get()));
        logger.info(String.format("read load count: %d", Util.readLogCount.get()));
        logger.info(String.format("out put line : %d", LogRebuilderLarge.outputCount.get()));
    }


    private static void doTest() throws Exception {
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
        buffer.flip();
        ResultWriter.writeBuffer(buffer);
    }


    private static class Worker implements Runnable {

        @Override
        public void run() {
            try {
                //运行我们的程序
                if (Config.TEST_MODE.equals("test")) {
                    doTest();
                } else if (Config.TEST_MODE.equals("real")) {
                    getResult();
                } else if (Config.TEST_MODE.equals("mix")) {
                    doTest();
                    getResult();
                } else {
                    throw new Exception("wrong test mode");
                }
                logger.info("send result to client");
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
