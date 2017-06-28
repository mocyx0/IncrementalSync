package org.pangolin.yx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import java.io.*;
import java.net.Socket;
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

    private static ArrayList<byte[]> waitBuff = new ArrayList<>();

    public synchronized static void clearWaitBuff() throws Exception {
        Socket client = NetServer.getClientSocket();
        for (byte[] preData : waitBuff) {
            client.getOutputStream().write(preData);
            //channel.writeAndFlush(byteBuf);
        }
        waitBuff.clear();

        /*
        Channel channel = NetServerHandler.getClientChannel();
        for (byte[] preData : waitBuff) {
            ByteBuf byteBuf = Unpooled.wrappedBuffer(preData, 0, preData.length);
            channel.writeAndFlush(byteBuf);
        }
        waitBuff.clear();
        */
    }

    public volatile static boolean writeDone = false;

    public volatile static int writeCount = 0;

    public synchronized static void close() throws Exception {
        Socket client = NetServer.getClientSocket();
        // MLog.info(String.format("write %d", writeCount));
        if (client != null) {
            try {
                client.getOutputStream().flush();
                client.close();
            } catch (Exception e) {
                MLog.info(e);
            }
        }
        writeDone = true;
    }

    public synchronized static void writeBuffer(ByteBuffer buffer) throws Exception {
        writeCount += buffer.limit();
        byte[] data = new byte[buffer.limit()];
        //MLog.info(String.format("write %d", buffer.limit()));
        buffer.get(data);
        Socket client = NetServer.getClientSocket();
        if (client == null) {
            waitBuff.add(data);
        } else {
            clearWaitBuff();
            client.getOutputStream().write(data);
            //client.getOutputStream().flush();
        }
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

        /*
        byte[] data = new byte[buffer.limit()];
        buffer.get(data);

        Channel channel = NetServerHandler.getClientChannel();
        if (channel == null) {
            waitBuff.add(data);
            // Config.serverLogger.info(String.format("client channel is empty, wait buff len %d", waitBuff.size()));
        } else {
            clearWaitBuff();
            if (buffer.limit() == 0) {
                MLog.info("buffer limit =0");
            }
            ByteBuf byteBuf = Unpooled.wrappedBuffer(data, 0, data.length);
            channel.writeAndFlush(byteBuf);
        }
        */
    }


}
