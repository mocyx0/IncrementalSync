package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


/**
 * Created by yangxiao on 2017/6/7.
 */
public class NetServerHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    public static ByteBuffer data;

    /**
     * 根据channel
     *
     * @param ctx
     * @return
     */
    public static String getIPString(ChannelHandlerContext ctx) {
        String ipString = "";
        String socketString = ctx.channel().remoteAddress().toString();
        int colonAt = socketString.indexOf(":");
        ipString = socketString.substring(1, colonAt);
        return ipString;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // 保存channel
        //Server.getMap().put(getIPString(ctx), ctx.channel());

        logger.info("channelRead");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
        result.readBytes(result1);
        String resultStr = new String(result1);
        // 接收并打印客户端的信息
        logger.info("Client said:" + resultStr);

        if (sData != null) {
            sendResult(sData);
        }

        /*
        String message = "hello!";
        ByteBuf byteBuf = Unpooled.wrappedBuffer(message.getBytes());
        ctx.channel().writeAndFlush(byteBuf).addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("Server发送消息成功！");
            }
        });
        */
        /*
        while (true) {
            // 向客户端发送消息
            String message = (String) getMessage();
            if (message != null) {
                Channel channel = Server.getMap().get("127.0.0.1");
                ByteBuf byteBuf = Unpooled.wrappedBuffer(message.getBytes());
                channel.writeAndFlush(byteBuf).addListener(new ChannelFutureListener() {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        logger.info("Server发送消息成功！");
                    }
                });
            }
        }
        */

    }

    private static volatile Channel clientChannel;


    private static ByteBuffer sData;

    public static Channel getClientChannel() {
        return clientChannel;
    }

    public static void sendResult(ByteBuffer data) {
        if (clientChannel != null) {
            //发送查询结果
            ByteBuf byteBuf = Unpooled.wrappedBuffer(data.array(), data.arrayOffset(), data.position());
            clientChannel.writeAndFlush(byteBuf);
            logger.info("send data done");
            /*
            ChannelFuture closeFuture = clientChannel.closeFuture();
            closeFuture.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    //session cleanup logic
                    logger.info("client close");
                    //我们不需要shutdown 由评测程序杀死server
                    //logger.info("server close");
                    //System.exit(0);
                }
            });
            */
        } else {
            logger.info("ERROR clientChannel is null");
            sData = data;
        }
    }

    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        logger.info("channelActive");
        clientChannel = ctx.channel();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private Object getMessage() throws InterruptedException {
        // 模拟下数据生成，每隔5秒产生一条消息
        Thread.sleep(5000);
        return "message generated in ServerDemoInHandler";
    }
}
