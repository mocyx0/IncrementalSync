package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.pangolin.xuzhe.test.IOPerfTest;
import org.pangolin.xuzhe.test.ReadingThread;
import org.pangolin.yx.nixu.NXServer;
import org.pangolin.yx.zhengxu.ZXServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;

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

    private static WorkerServer workerServer = new ZXServer();


    private static class Worker implements Runnable {


        @Override
        public void run() {
            try {
                //运行我们的程序
                if (Config.TEST_MODE.equals("test")) {
                    workerServer.doTest();

                } else if (Config.TEST_MODE.equals("real")) {
                    workerServer.doData();
                } else if (Config.TEST_MODE.equals("mix")) {
                    workerServer.doTest();
                    workerServer.doData();
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
                //query.end = 2000000;
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
