package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Client;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.pangolin.yx.zhengxu.ZXClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Created by yangxiao on 2017/6/7.
 */


public class NetClient {
    private static WorkerClient workerClient;
    private static byte[] buff = new byte[64 * 1024];

    public static void start(String host) throws Exception {

        int port = Config.SERVER_PORT;
        Socket client = null;
        while (true) {
            try {
                client = new Socket(host, port);
                break;
            } catch (IOException e) {
                MLog.info(e.toString());
                Thread.sleep(100);
            }
        }
        workerClient = new ZXClient();
        MLog.info("connected");
        //client.getOutputStream().write("hello".getBytes());
        InputStream is = client.getInputStream();
        while (true) {
            //ByteBuffer buff = ByteBuffer.allocate(60 * 1024);
            try {
                int len = is.read(buff);
                //MLog.info(String.format("recv %d", len));
                if (len == -1) {
                    break;
                } else {
                    ByteBuffer buf = ByteBuffer.wrap(buff, 0, len);
                    buf.position(len);
                    buf.flip();
                    workerClient.onData(buf, null);
                }
            } catch (Exception e) {
                MLog.info(e);
                break;
            }
        }
        workerClient.onClosed();

        /*
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
                MLog.info("try connect");
                try {
                    ChannelFuture f = b.connect(host, Config.SERVER_PORT).sync();
                    // Wait until the connection is closed.
                    f.channel().closeFuture().sync();
                    System.exit(0);
                } catch (Exception e) {
                    MLog.info(e.toString());

                }
                Thread.sleep(1000);
            }


        } finally {
            workerGroup.shutdownGracefully();
        }
        */
    }

}
