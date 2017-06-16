package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Client;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangxiao on 2017/6/7.
 */


public class NetClient {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void start(String host) throws Exception {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    // ch.pipeline().addLast(new IdleStateHandler(10, 0, 0));
                    //ch.pipeline().addLast(new ClientIdleEventHandler());
                    ch.pipeline().addLast(new NetClientHandler());
                }
            });

            // Start the client.
            int n = 10000;
            while (n > 0) {
                n--;
                logger.info("try connect");
                try {
                    ChannelFuture f = b.connect(host, Config.SERVER_PORT).sync();
                    // Wait until the connection is closed.
                    f.channel().closeFuture().sync();
                    System.exit(0);
                } catch (Exception e) {
                    logger.info("{}", e);

                }
                Thread.sleep(1000);
            }


        } finally {
            workerGroup.shutdownGracefully();
        }
    }

}
