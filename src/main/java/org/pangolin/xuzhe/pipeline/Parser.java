package org.pangolin.xuzhe.pipeline;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.pangolin.xuzhe.pipeline.ByteBufferPool.EMPTY_BUFFER;
import static org.pangolin.xuzhe.pipeline.Constants.STRING_LIST_SIZE;
import static org.pangolin.xuzhe.pipeline.Filter.logStringListQueue;
import static org.pangolin.xuzhe.pipeline.StringArrayListPool.EMPTY_STRING_LIST;

/**
 * Created by ubuntu on 17-6-3.
 */
public class Parser extends Thread {
	Logger logger = LoggerFactory.getLogger(Server.class);

	private Queue<ByteBuffer> buffers;
	private static AtomicInteger workerNum = new AtomicInteger(0);
	private int lineCnt = 0;
	private StringArrayListPool stringListPool = StringArrayListPool.getInstance();

	private ArrayList<String> currentStringList;
	public Parser() {
		super("Parser" + workerNum.incrementAndGet());
		this.buffers = new ConcurrentLinkedQueue<ByteBuffer>();
		try {
			currentStringList = stringListPool.get();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void appendBuffer(ByteBuffer buffer) {
		this.buffers.offer(buffer);
	}

	@Override
	public void run() {
		ByteBufferPool pool = ByteBufferPool.getInstance();
		final byte newLine = (byte)'\n';
		MyReverseOrderStringBuilder lineBuilder = new MyReverseOrderStringBuilder(300);
		try {
			while(true) {
				ByteBuffer buffer = this.buffers.poll();
				if(buffer == null) {
					sleep(5);
					continue;
				}
				if(buffer == EMPTY_BUFFER) {
					logStringListQueue.put(currentStringList);
					logStringListQueue.put(EMPTY_STRING_LIST);
					break;
				}

				long begin = System.nanoTime();
				byte[] data = buffer.array();
				int limit = buffer.limit();
				for(int i = limit-1; i>=0; --i) {
					byte b = data[i];
					if(b != newLine) {
						lineBuilder.append(b);
					} else if(lineBuilder.getSize() == 0) {
						// 空串，则跳过
					} else {
						String str = lineBuilder.toString();
						process(str);
						lineBuilder.clear();
					}
				}
				long end = System.nanoTime();
				pool.put(buffer);
			}
			System.out.println(lineCnt);
			//logger.info("{} done!", Thread.currentThread().getName());
		} catch (InterruptedException e) {
			logger.error("Parser was interrupted", e);
		} catch (Exception e) {
			logger.info("{}", e);
		}
	}

	private void process(String line) throws InterruptedException {
		if(currentStringList.size() == STRING_LIST_SIZE) {
			logStringListQueue.put(currentStringList);
			currentStringList = stringListPool.get();
		}
//		System.out.println(line);
		currentStringList.add(line);
		lineCnt++;
	}
}
