package org.pangolin.xuzhe.positiveorder;

import com.alibaba.middleware.race.sync.Client;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.pangolin.yx.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by ubuntu on 17-6-18.
 */
public class ClientResultReceiverHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(Client.class);
    FileOutputStream fileOutputStream = null;
    public ClientResultReceiverHandler() {

    }

    private static volatile Channel clientChannel;

    public static Channel getClientChannel() {
        return clientChannel;
    }


    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        logger.info("Client side, channelActive");
        String msg = "I am prepared to receive messages";
        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        ctx.write(encoded);
        ctx.flush();
        clientChannel = ctx.channel();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {


    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
        result.readBytes(result1);
        logger.info("Client side, channel Read:{}", result1.length);
        try {
            File f = new File(Config.RESULT_HOME + "/" + Config.RESULT_NAME);
            fileOutputStream = new FileOutputStream(f);
            fileOutputStream.write(result1);
            fileOutputStream.close();
//            Thread.sleep(10000); //  休眠10秒
            logger.info("File {} size:{}", f.getAbsolutePath(), f.length());
            System.exit(0);
        } catch (IOException e) {
            logger.info("", e);
        }

    }

    public static void sendResult(ByteBuf byteBuf) throws InterruptedException {

        while (clientChannel == null) {
            logger.info("client还未与Server建立连接，将等待10ms");
            Thread.sleep(10);
        }
        //发送查询结果
        clientChannel.writeAndFlush(byteBuf);
        logger.info("send data done");
    }
}