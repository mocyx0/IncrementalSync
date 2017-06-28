package org.pangolin.yx.nixu;

import com.alibaba.middleware.race.sync.Client;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.pangolin.yx.Config;
import org.pangolin.yx.MLog;
import org.pangolin.yx.WorkerClient;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Created by yangxiao on 2017/6/16.
 */
public class NXClient implements WorkerClient {

    private RandomAccessFile raf;

    private static class DumpBlock {
        long pos;
        int length;
    }

    public NXClient() throws Exception {
        String path = Config.RESULT_HOME + "/" + Config.RESULT_NAME;
        File f = new File(path);
        if (f.exists()) {
            f.delete();
        }
        raf = new RandomAccessFile(path, "rw");
    }

    private static int printLineCount = 0;

    private void dumpToFile() throws Exception {
        MLog.info(String.format("start dump recv size %d", writeOff));

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
                        MLog.info(ss[j]);
                        printLineCount++;
                        j++;
                    }
                }
                //raf.write(buffer.array(), (int) block.pos, block.length);
                raf.write(buffer, (int) block.pos, block.length);
            }
        }
        MLog.info(String.format("end dump,file size:%d", raf.length()));
        raf.close();

    }

    //
    private static int RECV_BUFF_SZIE = 1024 * 1024 * 512;
    private static byte[] buffer = new byte[RECV_BUFF_SZIE];
    private static int writeOff = 0;

    @Override
    public void onActive() throws Exception {

    }

    @Override
    public void onData(ByteBuffer result, Socket sock) throws Exception {
        /*
        int readlLen = result.readableBytes();
        result.readBytes(buffer, writeOff, result.readableBytes());
        writeOff += readlLen;
        if (writeOff > 0 && buffer[writeOff - 1] == 0) {
            dumpToFile();
            ctx.channel().close().sync();
            System.exit(0);
        }
        */
    }

    @Override
    public void onClosed() throws Exception {

    }
}
