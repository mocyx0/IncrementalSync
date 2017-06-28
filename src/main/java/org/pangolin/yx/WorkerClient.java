package org.pangolin.yx;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Created by yangxiao on 2017/6/16.
 */
public interface WorkerClient {

    void onActive() throws Exception;

    void onData(ByteBuffer data, Socket sock) throws Exception;
    void onClosed()throws Exception;
}
