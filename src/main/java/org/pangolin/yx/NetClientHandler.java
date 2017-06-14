package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Client;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;

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

    private static class DumpBlock {
        long pos;
        int length;
    }

    private static int printLineCount = 0;

    private void dumpToFile() throws Exception {
        logger.info(String.format("start dump recv size %d", writeOff));

        ByteBuffer bf = ByteBuffer.wrap(buffer);
        bf.position(writeOff);

        bf.flip();
        TreeMap<Integer, ArrayList<DumpBlock>> blocks = new TreeMap<>();
        while (true) {
            int index = bf.getInt();
            if (index == 0) {
                break;
            } else {
                int seq = bf.getInt();
                int size = bf.getInt();
                if (!blocks.containsKey(index)) {
                    blocks.put(index, new ArrayList<DumpBlock>());
                }
                ArrayList<DumpBlock> arr = blocks.get(index);
                DumpBlock newBlock = new DumpBlock();
                newBlock.pos = bf.position();
                newBlock.length = size;
                arr.add(newBlock);
                if (size > 1000) {
                    //logger.info(String.format("block size %d", bf.position() + size));
                }


                bf.position(bf.position() + size);
                //logger.info(String.format("index %d ,seq %d , block size %d , new pos %d", index, seq, size, bf.position()));
            }
        }
        //write to file
        for (Integer i : blocks.keySet()) {
            ArrayList<DumpBlock> arr = blocks.get(i);
            for (DumpBlock block : arr) {
                //打印一部分输出
                if (printLineCount < Config.PRINT_RESULT_LINE) {
                    String s = new String(buffer, (int) block.pos, block.length);
                    String[] ss = s.split("\\n");
                    int j = 0;
                    while (j < ss.length && printLineCount < Config.PRINT_RESULT_LINE) {
                        logger.info(ss[j]);
                        printLineCount++;
                        j++;
                    }
                }
                //raf.write(buffer.array(), (int) block.pos, block.length);
                raf.write(buffer, (int) block.pos, block.length);
            }
        }
        logger.info(String.format("end dump,file size:%d", raf.length()));
        raf.close();

    }

    //
    private static int RECV_BUFF_SZIE = 1024 * 1024 * 512;
    private static int packCount = 0;
    private static byte[] buffer = new byte[RECV_BUFF_SZIE];
    private static int writeOff = 0;

    // 接收server端的消息，并打印出来
    @Override
    public synchronized void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //logger.info("thread name " + Thread.currentThread().getName());

        ByteBuf result = (ByteBuf) msg;
        //logger.info(String.format("channelRead size:%d", result.readableBytes()));
        int readlLen = result.readableBytes();
        result.readBytes(buffer, writeOff, result.readableBytes());
        writeOff += readlLen;
        if (writeOff > 0 && buffer[writeOff - 1] == 0) {
            dumpToFile();
            ctx.channel().close().sync();
            System.exit(0);
        }
        /*
        if (buffer.position() > 0 && buffer.array()[buffer.position() - 1] == 0) {
            dumpToFile();
            ctx.channel().close().sync();
            System.exit(0);
        }
        */
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
