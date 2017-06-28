package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Server;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;


/**
 * Created by yangxiao on 2017/6/7.
 */
public class NetServer {

    private static ServerSocket serverSocket;

    public static Socket getClientSocket() {
        return client;
    }

    private static volatile Socket client;

    public static void start() throws Exception {
        MLog.info("NetServer start");
        int port = Config.SERVER_PORT;
        serverSocket = new ServerSocket(port);
        //serverSocket.setSoTimeout(10000);
        client = serverSocket.accept();
        MLog.info("connected");

        ResultWriter.clearWaitBuff();
        if (ResultWriter.writeDone == true) {
            client.close();
        }
        /*
        byte[] recvBuff = new byte[4096];
        while (true) {
            InputStream is = client.getInputStream();
            int c = is.read(recvBuff);
            MLog.info(String.format("recv %d", c));
        }
        */

        /*
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
                            ch.pipeline().addLast(new NetServerHandler());
                            // ch.pipeline().addLast(new ServerDemoOutHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(Config.SERVER_PORT).sync();
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
        */
    }

}
