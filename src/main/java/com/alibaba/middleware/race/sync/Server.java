package com.alibaba.middleware.race.sync;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.pangolin.xuzhe.positiveorder.ReadingThread;
import org.pangolin.xuzhe.positiveorder.ResultSenderHandler;
import org.pangolin.yx.Config;
import org.pangolin.yx.MServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import static com.alibaba.middleware.race.sync.Constants.SERVER_PORT;

/**
 * 服务器类，负责push消息到client Created by wanshao on 2017/5/25.
 */
public class Server {

    // 保存channel
    private static Map<String, Channel> map = new ConcurrentHashMap<String, Channel>();
    // 接收评测程序的三个参数
    private static String schema;
    private static Map tableNamePkMap;

    public static Map<String, Channel> getMap() {
        return map;
    }

    public static void setMap(Map<String, Channel> map) {
        Server.map = map;
    }

    private static void mainYX(String[] args) {
        MServer.main(args);
    }

    private static void mainXZ(String[] args) {
        Logger logger = LoggerFactory.getLogger(Server.class);
        try {
            logger.info("server start");
            Config.init();
            initProperties();

            printInput(logger, args);
            ReadingThread.beginId = Long.parseLong(args[2]);
            ReadingThread.endId = Long.parseLong(args[3]);
            String fileBaseName = Config.DATA_HOME + "/";
            int fileCnt = 0;
            for (int i = 1; i <= 10; i++) {
                String fileName = fileBaseName + i + ".txt";
                File f = new File(fileName);
                if (f.exists()) fileCnt++;
            }
            String[] fileNames = new String[fileCnt];
            for (int i = 1; i <= fileCnt; i++) {
                fileNames[i - 1] = fileBaseName + i + ".txt";
                logger.info("fileName:{}", fileNames[i - 1]);
            }
            long time1 = System.currentTimeMillis();
            ReadingThread readingThread = new ReadingThread(fileNames);
            readingThread.start();

            Server server = new Server();
            logger.info("com.alibaba.middleware.race.sync.Server is running....");

            server.startServer(SERVER_PORT);
            readingThread.join();
            long time2 = System.currentTimeMillis();
            System.out.println("elapsed time:" + (time2 - time1) + "ms");
        } catch (Exception e) {
            logger.info("{}", e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
//        mainXZ(args);
        mainYX(args);
    }

    /**
     * 打印赛题输入 赛题输入格式： schemaName tableName startPkId endPkId，例如输入： middleware student 100 200
     * 上面表示，查询的schema为middleware，查询的表为student,主键的查询范围是(100,200)，注意是开区间 对应DB的SQL为： select * from middleware.student where
     * id>100 and id<200
     */
    private static void printInput(Logger logger, String[] args) {
        // 第一个参数是Schema Name
        logger.info("Schema:" + args[0]);
        // 第二个参数是Schema Name
        logger.info("table:" + args[1]);
        // 第三个参数是start pk Id
        logger.info("start:" + args[2]);
        // 第四个参数是end pk Id
        logger.info("end:" + args[3]);

    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }


    private void startServer(int port) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // 注册handler
                            ch.pipeline().addLast(new ResultSenderHandler());
                            // ch.pipeline().addLast(new ServerDemoOutHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
