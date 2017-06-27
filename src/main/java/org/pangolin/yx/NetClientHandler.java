package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Client;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.pangolin.yx.zhengxu.ZXClient;


/**
 * Created by yangxiao on 2017/6/7.
 */
public class NetClientHandler extends ChannelInboundHandlerAdapter {

    private static WorkerClient workerClient;


    NetClientHandler() throws Exception {
        workerClient = new ZXClient();

    }


    // 接收server端的消息，并打印出来
    @Override
    public synchronized void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //logger.info("thread name " + Thread.currentThread().getName());

        ByteBuf result = (ByteBuf) msg;
        //logger.info(String.format("channelRead size:%d", result.readableBytes()));
        workerClient.onData(result, ctx);
        result.release();
    }

    private static ByteBuf stringToBuffer(String msg) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(msg.getBytes());
        return byteBuf;
    }

    // 连接成功后，向server发送消息
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        MLog.info("channelActive");

        workerClient.onActive();

        String msg = "I am prepared to receive messages";
        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        ctx.write(encoded);
        ctx.flush();
    }
}
