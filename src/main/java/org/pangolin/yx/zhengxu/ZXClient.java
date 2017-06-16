package org.pangolin.yx.zhengxu;

import com.alibaba.middleware.race.sync.Client;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.pangolin.yx.WorkerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yangxiao on 2017/6/16.
 */
public class ZXClient implements WorkerClient {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    @Override
    public void onActive() throws Exception {

    }

    @Override
    public void onData(ByteBuf data, ChannelHandlerContext ctx) throws Exception {
        logger.info("hello");
        ctx.channel().close().sync();
        System.exit(0);
    }
}
