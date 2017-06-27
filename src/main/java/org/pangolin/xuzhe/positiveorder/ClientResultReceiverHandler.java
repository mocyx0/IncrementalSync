package org.pangolin.xuzhe.positiveorder;

import com.alibaba.middleware.race.sync.Client;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.pangolin.yx.Config;
import org.pangolin.yx.MLog;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;

/**
 * Created by ubuntu on 17-6-18.
 */
public class ClientResultReceiverHandler extends ChannelInboundHandlerAdapter {
    FileOutputStream fileOutputStream = null;
    private boolean ok = false;

    public ClientResultReceiverHandler() {

    }

    private static volatile Channel clientChannel;

    public static Channel getClientChannel() {
        return clientChannel;
    }


    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        MLog.info("Client side, channelActive");
        String msg = "I am prepared to receive messages";
        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        ctx.write(encoded);
        ctx.flush();
        clientChannel = ctx.channel();
        ok = false;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (ok) {
            ctx.close();
        } else {
            ctx.read();
        }

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
        result.readBytes(result1);
//        logger.info("Client side, channel Read:{}", result1.length);
        try {
            File f = new File(Config.RESULT_HOME + "/" + Config.RESULT_NAME);
            fileOutputStream = new FileOutputStream(f);
            fileOutputStream.write(result1);
            fileOutputStream.close();
//            Thread.sleep(10000); //  休眠10秒
//            logger.info("File {} size:{}  MD5:{}", f.getAbsolutePath(), f.length(), MD5(result1));
            MLog.info("File {} size:{} " + f.getAbsolutePath() + f.length());
            ok = true;
            System.exit(0);
        } catch (IOException e) {
            MLog.info("" + e.toString());
        }

    }

    private static char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public final static String MD5(byte[] result) {
        try {
            byte[] btInput = result;
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}