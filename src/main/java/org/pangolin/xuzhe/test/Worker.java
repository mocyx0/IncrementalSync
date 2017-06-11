package org.pangolin.xuzhe.test;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.pangolin.xuzhe.test.Constants.LINE_MAX_LENGTH;

/**
 * Created by ubuntu on 17-6-3.
 */
public class Worker extends Thread {
    Logger logger = LoggerFactory.getLogger(Server.class);
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private Queue<Block> buffers;
    private static AtomicInteger workerNum = new AtomicInteger(0);
    public Map<Integer, Map<String, AtomicLong>> tableLogCountMap = new HashMap<>();
    public Map<Integer, Map<String, AtomicLong>> opCountMap = new HashMap<>();
    public Map<Integer, AtomicLong> lineCountMap = new HashMap<>();

    public Worker() {
        super("Worker" + workerNum.incrementAndGet());
        this.buffers = new ConcurrentLinkedQueue<Block>();

    }

    public void appendBuffer(ByteBuffer buffer, long pos, int fileNo) {
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
                int len = -1;
                while(buffer.hasRemaining()) {
                    byte b = buffer.get();
                    if(b != newLine) {
                        bytes[++len] = b;
                    } else {
						try {
							String str = new String(bytes, 0, len, "utf-8");
							process(str, block.getFileNo());
							len = -1;
						} catch (UnsupportedEncodingException e) {

						}
                    }
                }
                long end = System.nanoTime();
                pool.put(buffer);
            }
            //logger.info("{} done!", Thread.currentThread().getName());
        } catch (InterruptedException e) {
            logger.error("Worker was interrupted", e);
        } catch (Exception e) {
        	logger.info("{}", e);
		}

    }


    // 统计不同文件中的信息 使用两层HashMap
    private void process(String line, int fileNo) {
		Map<String, AtomicLong> map = tableLogCountMap.get(fileNo);
    	if(map == null) {
    		map = new HashMap<String, AtomicLong>();
    		tableLogCountMap.put(fileNo, map);
		}
		String[] items = line.split("\\|");
		String database = items[3];
		String table = items[4];
		String op = items[5];
		String dbTableName = database+"|"+table;
		AtomicLong atomicLong = map.get(dbTableName);
		if(atomicLong == null) {
			atomicLong = new AtomicLong(0);
			map.put(dbTableName, atomicLong);
		}
		atomicLong.incrementAndGet();

		// 统计每种操作的数量
		map = opCountMap.get(fileNo);
		if(map == null) {
			map = new HashMap<String, AtomicLong>();
			opCountMap.put(fileNo, map);
		}
		atomicLong = map.get(op);
		if(atomicLong == null) {
			atomicLong =new AtomicLong(0);
			map.put(op, atomicLong);
		}
		atomicLong.incrementAndGet();


		// 统计每个文件的行数
		atomicLong = lineCountMap.get(fileNo);
		if(atomicLong == null) {
			atomicLong =new AtomicLong(0);
			lineCountMap.put(fileNo, atomicLong);
		}
		atomicLong.incrementAndGet();
    }

	public static class Block {
		private final long position;
		private final int fileNo;
		private final ByteBuffer buffer;
		public Block(ByteBuffer buf, long pos, int fileNo) {
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
