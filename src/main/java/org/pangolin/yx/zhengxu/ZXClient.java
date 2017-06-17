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

/**
 * Created by yangxiao on 2017/6/16.
 */
public class ZXClient implements WorkerClient {
    private static Logger logger = LoggerFactory.getLogger(Client.class);
    private RandomAccessFile raf;

    public ZXClient() throws Exception {
        String path = Config.RESULT_HOME + "/" + Config.RESULT_NAME;
        File f = new File(path);
        if (f.exists()) {
            f.delete();
        }
        raf = new RandomAccessFile(path, "rw");
    }

    @Override
    public void onActive() throws Exception {
        logger.info("onActive");
    }

    @Override
    public void onData(ByteBuf data, ChannelHandlerContext ctx) throws Exception {
        logger.info("onData");
        raf.write('1');
        raf.close();
        logger.info("closed");
        ctx.channel().close().sync();
        System.exit(0);
    }
}
