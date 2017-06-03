package org.pangolin.xuzhe;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.pangolin.xuzhe.Constants.LINE_MAX_LENGTH;

/**
 * Created by ubuntu on 17-6-3.
 */
public class Worker extends Thread {
    Logger logger = LoggerFactory.getLogger(Worker.class);
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private BlockingQueue<ByteBuffer> buffers;
    private static AtomicInteger workerNum = new AtomicInteger(0);
    public Worker() {
        super("Worker" + workerNum.incrementAndGet());
        this.buffers = new ArrayBlockingQueue<ByteBuffer>(20, true);
    }

    public void appendBuffer(ByteBuffer buffer) throws InterruptedException {
        this.buffers.put(buffer);
    }

    @Override
    public void run() {
        ByteBufferPool pool = ByteBufferPool.getInstance();
        byte[] bytes = new byte[LINE_MAX_LENGTH];
        final byte newLine = (byte)'\n';
        try {
            while(true) {
                ByteBuffer buffer = this.buffers.take();
                if(buffer == EMPTY_BUFFER) break;
                long begin = System.nanoTime();
                int len = 0;
                while(buffer.hasRemaining()) {
                    byte b = buffer.get();
                    if(b != newLine) {
                        bytes[len++] = b;
                    } else {
                        process(bytes, len);
                        len = 0;
                    }
                }
                long end = System.nanoTime();
                // 将日志注释会加速处理速度
//                logger.info("{} process elapsed time:{} ns", Thread.currentThread().getName(), (end-begin));
                pool.put(buffer);
            }
        } catch (InterruptedException e) {
            logger.error("Worker was interrupted", e);
        }

    }

    private void process(byte[] bytes, int len) {

    }
}
