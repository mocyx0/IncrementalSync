package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Client;
import com.alibaba.middleware.race.sync.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Created by yangxiao on 2017/6/7.
 */
public class NetClientHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(Client.class);
    RandomAccessFile raf;

    NetClientHandler() {
        try {
            String path = Config.RESULT_HOME + "/" + Config.RESULT_NAME;
            File f = new File(path);
            if (f.exists()) {
                f.delete();
            }
            raf = new RandomAccessFile(path, "rw");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

    }


    private void writeToFile(byte[] data) throws Exception {
        int len = data.length;
        if (data[data.length - 1] == 0) {
            len = data.length - 1;
        }
        raf.write(data, 0, len);
    }

    ByteBuffer testBuffer = ByteBuffer.allocate(1024 * 1024);

    // 接收server端的消息，并打印出来
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logger.info("channelRead");

        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        result.readBytes(result1);

        writeToFile(result1);
        if (result1[result1.length - 1] == 0) {
            //end
            testBuffer.put(result1, 0, result1.length - 1);
            String testStr = new String(testBuffer.array(), 0, testBuffer.position());
            logger.info(testStr);
            raf.close();
            //关闭连接
            ctx.channel().close().sync();
            System.exit(0);

        } else {
            testBuffer.put(result1, 0, result1.length);
        }
//        System.out.println("Server said:" + new String(result1));
        result.release();

        //ctx.writeAndFlush("I have received your messages and wait for next messages");
        //ctx.write(stringToBuffer("hi!"));
        //ctx.channel().writeAndFlush(stringToBuffer("hi!"));


    }

    private static ByteBuf stringToBuffer(String msg) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(msg.getBytes());
        return byteBuf;
    }

    // 连接成功后，向server发送消息
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("channelActive");

        String msg = "I am prepared to receive messages";
        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        ctx.write(encoded);
        ctx.flush();
    }
}
