package org.pangolin.yx;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;

/**
 * Created by yangxiao on 2017/6/16.
 */
public interface WorkerClient {

    void onActive() throws Exception;

    void onData(ByteBuf data, ChannelHandlerContext ctx) throws Exception;

}
