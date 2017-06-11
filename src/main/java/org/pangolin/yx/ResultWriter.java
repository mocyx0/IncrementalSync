package org.pangolin.yx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/5.
 */

public class ResultWriter {
    /*
    public static ByteBuffer writeToBuffer(RebuildResult result) {
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024 * 4);
        for (ArrayList<String> strs : result.datas) {
            for (int i = 0; i < strs.size(); i++) {
                buffer.put(strs.get(i).getBytes());
                if (i != strs.size() - 1) {
                    buffer.put((byte) '\t');
                }
            }
            buffer.put((byte) '\n');
        }
        buffer.put((byte) 0);
        return buffer;
    }

    public static void writeToFile(RebuildResult result) throws Exception {
        String path = Config.RESULT_HOME + "/" + Config.RESULT_NAME;
        File f = new File(path);
        if (f.exists()) {
            f.delete();
        }
        RandomAccessFile raf = new RandomAccessFile(path, "rw");
        BufferedWriter writer = new BufferedWriter(new FileWriter(raf.getFD()));
        for (ArrayList<String> strs : result.datas) {
            for (int i = 0; i < strs.size(); i++) {
                writer.write(strs.get(i));
                if (i != strs.size() - 1) {
                    writer.write('\t');
                }
            }
            writer.write('\n');
        }
        writer.close();
    }
    */

    private static RandomAccessFile raf = null;

    public synchronized static void writeBuffer(ByteBuffer buffer) throws Exception {
        /*
        if (Config.SINGLE) {
            if (raf == null) {
                String path = Config.RESULT_HOME + "/" + Config.RESULT_NAME;
                File f = new File(path);
                if (f.exists()) {
                    f.delete();
                }
                raf = new RandomAccessFile(path, "rw");
            }
            if (buffer.limit() != 0) {
                raf.write(buffer.array(), buffer.arrayOffset(), buffer.limit());
            } else {
                raf.close();
            }
        } else {
        */
        Channel channel = NetServerHandler.getClientChannel();
        if (channel == null) {
            Config.serverLogger.info("client channel is empty");
        } else {
            if (buffer.limit() == 0) {
                Config.serverLogger.info("buffer limit =0");
            }
            ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.array(), buffer.arrayOffset(), buffer.limit());
            channel.writeAndFlush(byteBuf);
            Config.serverLogger.info("channel send data");
        }
    }


}
