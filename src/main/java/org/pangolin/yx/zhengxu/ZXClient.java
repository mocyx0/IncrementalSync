package org.pangolin.yx.zhengxu;

import com.alibaba.middleware.race.sync.Client;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.pangolin.yx.Config;
import org.pangolin.yx.WorkerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Created by yangxiao on 2017/6/16.
 */
public class ZXClient implements WorkerClient {
    private Logger logger;
    private RandomAccessFile raf;

    private static class DumpBlock {
        long pos;
        int length;
    }

    public ZXClient() throws Exception {
        logger = LoggerFactory.getLogger(Client.class);
        String path = Config.RESULT_HOME + "/" + Config.RESULT_NAME;
        File f = new File(path);
        if (f.exists()) {
            f.delete();
        }
        raf = new RandomAccessFile(path, "rw");
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

                if (bf.position() + size > bf.limit()) {
                    System.out.print(1);
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
    private static int RECV_BUFF_SZIE = 1024 * 1024 * 128;
    private static byte[] buffer = new byte[RECV_BUFF_SZIE];
    private static int writeOff = 0;

    @Override
    public void onActive() throws Exception {

    }

    @Override
    public void onData(ByteBuf result, ChannelHandlerContext ctx) throws Exception {
        logger.info("onData");

        int readlLen = result.readableBytes();
        result.readBytes(buffer, writeOff, result.readableBytes());
        writeOff += readlLen;
        if (writeOff > 0 && buffer[writeOff - 1] == 0) {
            dumpToFile();
            ctx.channel().close().sync();
            System.exit(0);
        }
    }
}
