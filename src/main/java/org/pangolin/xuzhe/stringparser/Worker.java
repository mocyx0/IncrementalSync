package org.pangolin.xuzhe.stringparser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.pangolin.xuzhe.stringparser.Constants.LINE_MAX_LENGTH;

/**
 * Created by ubuntu on 17-6-3.
 */
public class Worker extends Thread {
    Logger logger = LoggerFactory.getLogger(Worker.class);
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private Queue<Block> buffers;
    private static AtomicInteger workerNum = new AtomicInteger(0);
	private LocalLogIndex localIndex = new LocalLogIndex();

    public Worker() {
        super("Worker" + workerNum.incrementAndGet());
        this.buffers = new ConcurrentLinkedQueue<Block>();

    }

    public void appendBuffer(ByteBuffer buffer, int pos, int fileNo) {
    	Block block = new Block(buffer, pos, fileNo);
        this.buffers.offer(block);
    }
    
	@Override
    public void run() {
        ByteBufferPool pool = ByteBufferPool.getInstance();
        byte[] bytes = new byte[LINE_MAX_LENGTH];                     //一条日志最长有多少个字节？
        final byte newLine = (byte)'\n';
        try {
            while(true) {
            	Block block = this.buffers.poll();
            	if(block == null) {
            		sleep(10);
            		continue;
				}
//				logger.info("{} buffer.size:{}", getName(), buffers.size());
            	ByteBuffer buffer = block.buffer;
				if(buffer == EMPTY_BUFFER) break;
//				pool.put(buffer);

				long begin = System.nanoTime();
                int len = 0;
                int startPos = block.position;
                while(buffer.hasRemaining()) {
                    byte b = buffer.get();
                    if(b != newLine) {
                        bytes[len] = b;
						++len;
                    } else {
                    	int pos = (startPos + buffer.position() - len - 1);
						try {
							String str = new String(bytes, 0, len, "utf-8");
							process(str, block.getFileNo(), pos);
							len = 0;
						} catch (UnsupportedEncodingException e) {

						}
                    }
                }
                long end = System.nanoTime();
                pool.put(buffer);
            }
            logger.info("{} done!", Thread.currentThread().getName());
        } catch (InterruptedException e) {
            logger.error("Worker was interrupted", e);
        }

    }


    private void process(String line, int fileNo, int position) {
		LogParser.parseToIndex(line, fileNo, position, localIndex);
	}

	public LocalLogIndex getIndexes() {
		return localIndex;
	}

	public static class Block {
		private final int position;
		private final int fileNo;
		private final ByteBuffer buffer;
		public Block(ByteBuffer buf, int pos, int fileNo) {
			position = pos;
			buffer = buf;
			this.fileNo = fileNo;
		}
		public long getPosition() {
			return position;
		}
		public int getFileNo() {
			return fileNo;
		}
		public ByteBuffer getBuffer() {
			return buffer;
		}

	}

}
