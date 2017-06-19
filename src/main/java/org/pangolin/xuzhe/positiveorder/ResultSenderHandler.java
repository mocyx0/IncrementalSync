package org.pangolin.xuzhe.positiveorder;

import com.alibaba.middleware.race.sync.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Created by ubuntu on 17-6-18.
 */
public class ResultSenderHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    public static CountDownLatch latch = new CountDownLatch(1);
    public static volatile ByteBuf byteBuf;
    private static volatile Channel clientChannel;
    public static Channel getClientChannel() {
        return clientChannel;
    }


    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        logger.info("channelActive");
        clientChannel = ctx.channel();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logger.info("server side, channel read. ");
//        byte[] data = "Hello".getBytes();
//        ByteBuf byteBuf = Unpooled.buffer(1000);
//        byteBuf.writeInt(data.length);
//        System.out.println(data.length);
//        byteBuf.writeBytes(data);
        latch.await();
        ctx.channel().writeAndFlush(byteBuf).addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("Server发送消息成功！ byte length:{}", byteBuf.writerIndex()-4);
            }
        });
    }

    public static void sendResult(ByteBuf byteBuf) throws InterruptedException {

        while(clientChannel == null) {
            logger.info("client还未与Server建立连接，将等待10ms");
            Thread.sleep(1000);
        }
        //发送查询结果
        int len = byteBuf.getInt(0);
        ResultSenderHandler.byteBuf = byteBuf;
        latch.countDown();
//        clientChannel.writeAndFlush(byteBuf);
        logger.info("send data done");
    }
}