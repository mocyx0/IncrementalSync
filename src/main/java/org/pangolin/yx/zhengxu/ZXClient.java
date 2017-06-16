package org.pangolin.yx.zhengxu;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.pangolin.yx.WorkerClient;

/**
 * Created by yangxiao on 2017/6/16.
 */
public class ZXClient implements WorkerClient {
    @Override
    public void onActive() throws Exception {

    }

    @Override
    public void onData(ByteBuf data, ChannelHandlerContext ctx) throws Exception {

    }
}
