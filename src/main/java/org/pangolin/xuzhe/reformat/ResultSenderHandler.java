package org.pangolin.xuzhe.reformat;

import com.alibaba.middleware.race.sync.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Created by ubuntu on 17-6-18.
 */
public class ResultSenderHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    public static CountDownLatch latch = new CountDownLatch(1);
    public static BlockingQueue<ByteBuf> resultQueue = new ArrayBlockingQueue<ByteBuf>(50);


    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        logger.info("client与Server建立连接成功" + ctx);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//        ctx.flush();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        logger.info("server side, channel read. {} ctx:{}", msg, ctx);
        latch.await();
//        while(true) {
            ByteBuf buf = resultQueue.take();

            if(buf.capacity() == 0) {
                ByteBuf endBuf = Unpooled.buffer(6);
                endBuf.writeInt(1);
                endBuf.writeByte(0);
//                logger.info("Server发送结束消息！ ctx:{}", ctx);
                ctx.channel().writeAndFlush(endBuf).addListener(new ChannelFutureListener() {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        logger.info("Server发送消息完成！");
//                        System.exit(0);
                    }
                });
            } else {
                final int size = buf.writerIndex();
                ctx.channel().writeAndFlush(buf).addListener(new ChannelFutureListener() {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
//                        logger.info("Server发送消息成功！ byte length:{}", size - 4);
                    }
                });
            }
//        }
    }
}
