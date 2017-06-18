package org.pangolin.xuzhe.positiveorder;

import com.alibaba.middleware.race.sync.Server;
import org.pangolin.xuzhe.stringparser.MyStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.pangolin.xuzhe.positiveorder.Constants.*;
import static org.pangolin.xuzhe.positiveorder.ReadBufferPool.EMPTY_BUFFER;

/**
 * Created by ubuntu on 17-6-3.
 */
public class Parser extends Thread {
	Logger logger = LoggerFactory.getLogger(Server.class);

	private BlockingQueue<ByteBuffer> buffers;
	private ArrayBlockingQueue<LogIndex>[] logIndexBlockingQueueArray;
	private int parserNo;
	public int readLineCnt = 0;
	public int readBytesCnt = 0;
	LogIndexPool logIndexPool;
	private Schema schema;
	public Parser(int parserNo) {
		this.setName("Parser" + parserNo);
		this.parserNo = parserNo;
		this.buffers = new ArrayBlockingQueue<ByteBuffer>(PARSER_BLOCKING_QUEUE_SIZE);
		logIndexBlockingQueueArray = new ArrayBlockingQueue[REDO_NUM];
		for(int i = 0; i < REDO_NUM; i++) {
			logIndexBlockingQueueArray[i] = new ArrayBlockingQueue<LogIndex>(PARSER_BLOCKING_QUEUE_SIZE);
		}

	}



	public LogIndex getLogIndexQueueHeaderByRedoId(int redoId) throws InterruptedException {
		LogIndex index =  logIndexBlockingQueueArray[redoId].take();
//		System.out.println(Thread.currentThread().getName() + " take a LogIndex");
		return index;
	}

	public void appendBuffer(ByteBuffer buffer) throws InterruptedException {
		this.buffers.put(buffer);
//		System.out.println("ReadingThread append a ReadBuffer");
	}

	@Override
	public void run() {
		ReadBufferPool pool = ReadBufferPool.getInstance();
		byte[] bytes = new byte[LINE_MAX_LENGTH];                     //一条日志最长有多少个字节？
		final byte newLine = (byte)'\n';
		try {
			ReadingThread.parserLatch.await();
			logIndexPool = LogIndexPool.getInstance();
			while(true) {
				ByteBuffer buffer = this.buffers.take();
//				logger.info("{} buffer.size:{}", getName(), buffers.size());
				if(buffer == EMPTY_BUFFER) {
					for(int r = 0; r < REDO_NUM; r++) {
						logIndexBlockingQueueArray[r].put(LogIndex.EMPTY_LOG_INDEX);
					}
					break;
				}

				long begin = System.nanoTime();
				process(buffer);

				long end = System.nanoTime();
//				pool.put(buffer);
			}
			//logger.info("{} done!", Thread.currentThread().getName());
		} catch (InterruptedException e) {
			logger.error("Worker was interrupted", e);
		} catch (Exception e) {
			logger.info("{}", e);
		}

	}

	private void process(ByteBuffer buffer) throws InterruptedException {
		int itemIndex = 0;
		int dataBegin = buffer.position();
		int dataEnd = buffer.limit();
//		int dataSize = (dataEnd - dataBegin);
//		int lastReadBytesCnt = readBytesCnt;
//		readBytesCnt += (dataEnd - dataBegin);
		int lineBegin = buffer.position();
		int lineEnd = -1;
		int subBegin = -1;
		byte[] data = buffer.array();
		byte op = 0;
		long oldPK = -1, newPK = -1;
		int hash = 0;
		int i = 0;
		byte b;
		LogIndex logIndex = logIndexPool.get();
		logIndex.setByteBuffer(buffer);

		int logItemIndex = 0;
		// 以 |mysql-bin.00001717148769|1496736165000|middleware3|student|I|id:1:1|NULL|11
		// |first_name:2:0|NULL|阮|last_name:2:0|NULL|甲|sex:2:0|NULL|女|score:1:0|NULL|53|
		for (i = dataBegin; i < dataEnd; ++i) {
			b = data[i];
//			char ch = (char) b;
			if (b == '|') {
				++itemIndex;
				if (itemIndex == 5) {
					++i;
					op = data[i];//I|id:1:1|NULL|11|first_na...

					if (op != 'I' && op != 'U' && op != 'D')
						throw new RuntimeException("op error:" + (char) op);
					// 直接跳过  id:1:1
					i += 9; // after:  NULL|11|first_na..
					if (op == 'I') {
						oldPK = -1;
						i += 5;  // after: 11|first_name:2:0|NULL|阮
					} else {
//						oldPK = data[i++];
						oldPK = 0;
						while ((b = data[i]) != '|') {
							oldPK = oldPK * 10 + (b - '0');
							++i;
						}
						++i;
					}
					if(op == 'D') {
						newPK = -1;
					} else {
						newPK = 0;
						while ((b = data[i]) != '|') {
							newPK = newPK * 10 + (b - '0');
							++i;
						}
					}
//					if(oldPK != -1 && oldPK != newPK) {
//						System.out.println(oldPK);
//					}
//					if( oldPK != -1 && newPK != -1 &&(oldPK < 0 || newPK < 0))
//						System.out.println();
					--i; // after:
					itemIndex += 3;
					String s = new String(data, lineBegin, i-lineBegin+1);
					logIndex.addNewLog(oldPK, newPK, op, logItemIndex);
				} else if (itemIndex == 9) {
					// current: |first_name:2:0|NULL|阮|...
					// 除PK以外，第一列的开始
//					buffer.position(i);
//					printSubLine(buffer);
					int[] hashs = logIndex.getHashColumnName(logItemIndex);
					int[] columnNewValues = logIndex.getColumnNewValues(logItemIndex);
					short[] columnValueLens = logIndex.getColumnValueLens(logItemIndex);
					int columnIndex = 0;
					while(data[i+1] != '\n') {
						// 计算column name的hash code
						++i;
						hash = 0;
						while ((b = data[i]) != ':') {
							hash = 31 * hash + b;
//						System.out.print((char)b);
							++i;
						} // after: :2:0|NULL|阮|...
//					System.out.println();
						hashs[columnIndex] = schema.columnHash2NoMap.get(hash).intValue();

						i += 4; // after: |NULL|阮|last_name:2:0...
						// 开始解析列的 新旧值
						if (op == 'I') {
							i += 5; // 插入操作的旧值为null，直接跳过
							        // after: |阮|last_name:2:0...
						} else {
							++i;
							int oldValueBegin = i;
							while ((b = data[i]) != '|') {
								++i;
							}
							int oldValueEnd = i;

						}
						++i; // after: 阮|last_name:2:0...
						int newValueBegin = i;
						while ((b = data[i]) != '|') {
							++i;
						}
						int newValueEnd = i;
						columnNewValues[columnIndex] = newValueBegin;
						columnValueLens[columnIndex] = (short)(newValueEnd-newValueBegin);
						++columnIndex;
					}
					logIndex.setColumnSize(logItemIndex, columnIndex);
				} else {

				}
			} else if (b == '\n') {
				// 处理完一行了
				lineEnd = i;
				++readLineCnt;
				readBytesCnt += (lineEnd - lineBegin + 1);
				lineBegin = lineEnd + 1;
				itemIndex = 0;
				++logItemIndex;
			}
		}
		logIndex.setLogSize(logItemIndex);
		for(int j = 0; j < REDO_NUM; j++) {
			logIndexBlockingQueueArray[j].put(logIndex);
//			System.out.println(getName() + " put a LogIndex into LogIndexQueue");
		}
//		logIndexPool.put(logIndex);
//		int currentReadBytesCnt = readBytesCnt-lastReadBytesCnt;
//		if(currentReadBytesCnt != dataSize) {
//			i++;
//		}
//		i++;
//		while(buffer.hasRemaining()) {
//			byte b = buffer.get(); // 改成直接对array操作试一下
//			if(b == '|') {
//				++itemIndex;
//				if(itemIndex == 5) {
//					// TODO 统计(i-begin)的值,方便后面加速跳过前面几列数据，直接定位到op列的位置
////					printSubLine(buffer);
//					op = buffer.get(); // TODO 为什么加了这一句  运行时间就从1100ms变成了1800ms？
//					if(op != 'I' && op != 'U' && op != 'D')
//						throw new RuntimeException("op error:" + (char)op);
//				} else if(itemIndex == 6) {
//					// id:1:1
//				} else if(itemIndex == 7) {
////					printSubLine(buffer);
////					long oldPK = 0;
////					while((b = buffer.get()) != '|') {
////						oldPK = oldPK*10 + (b-'0');
////					}
////					buffer.
//				}
//			}
//			if(b == '\n') {
//				// 处理完一行了
//				lineEnd = buffer.position();
////				byte[] subRaw = new byte[lineEnd - subBegin];
////				int mark = buffer.position();
////				buffer.position(subBegin);
////				buffer.get(subRaw);
////				buffer.position(mark);
////				String s2 = new String(subRaw);
////				System.out.println(s2);
//				lineBegin = lineEnd;
//
//				itemIndex = 0;
//			}
//
//		}


//		byte[] raw = new byte[end - begin];
//		byte[] subRaw = new byte[end - i];
//		int mark = currentBuffer.position();
//		currentBuffer.position(rawBegin);
//		currentBuffer.get(raw);
//		currentBuffer.position(rawBegin+(i-begin));
//		currentBuffer.get(subRaw);
//		currentBuffer.position(mark);
//		String s = new String(raw);
//		String s2 = new String(subRaw);
//		System.out.println(s2);
//		++readLineCnt;
//		readByteCnt += (end-begin+1);
	}

	private void printSubLine(ByteBuffer buffer) {
		int mark = buffer.position();
		byte[] subRaw = new byte[50];
		buffer.get(subRaw);
		buffer.position(mark);
		String s2 = new String(subRaw);
		System.out.println(s2);
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public int getParserNo() {
		return parserNo;
	}
}
