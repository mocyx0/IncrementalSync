package com.alibaba.middleware.race.sync;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.pangolin.xuzhe.reformat.ClientResultReceiverHandler;
import org.pangolin.yx.Config;
import org.pangolin.yx.MClient;
import org.pangolin.yx.MLog;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.ConnectException;

/**
 * Created by wanshao on 2017/5/25.
 */
public class Client {
    private final static int port = Constants.SERVER_PORT;
    // idle时间
    private static String ip;
    private EventLoopGroup loop = new NioEventLoopGroup();


    private static void mainYX(String[] args) {
        MClient.main(args);
    }

    private static void mainXZ(String[] args) throws Exception {

        MLog.info("mclient start");
        Config.init();
        initProperties();

        MLog.info("args: ");
        for (String s : args) {
            MLog.info(s);
        }
        MLog.info("Welcome to Client");
        // 从args获取server端的ip
        ip = args[0];
        Client client = new Client();
        client.connect(ip, port);
//        */
    }

    public static void main(String[] args) throws Exception {
        MLog.init("/home/admin/logs/7250941rrv/client-custom.log");
//        mainXZ(args);
        mainYX(args);
    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }

    /**
     * 连接服务端
     *
     * @param host
     * @param port
     * @throws Exception
     */
    public void connect(String host, int port) throws Exception {
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            public void initChannel(SocketChannel ch) throws Exception {
//                    ch.pipeline().addLast(new IdleStateHandler(10, 0, 0));
//                    ch.pipeline().addLast(new ClientIdleEventHandler());
                ch.pipeline().addFirst("decoder", new LengthFieldBasedFrameDecoder(1000000000, 0, 4, 0, 4));
                ch.pipeline().addLast(new ClientResultReceiverHandler());
            }
        });

        try {
            // Start the client.
            String lastError = null;
            while (true) {
                try {
                    ChannelFuture f = b.connect(host, port).sync();

                    // Wait until the connection is closed.
                    f.channel().closeFuture().sync();
                    Thread.sleep(1000);
                } catch (Exception e) {
                    if (lastError == null || !lastError.equals(e.getMessage())) {
                        lastError = e.getMessage();
                        MLog.info(lastError);
                    }
                    Thread.sleep(50);
                }
            }
        } finally {
            workerGroup.shutdownGracefully();
        }

    }


}
