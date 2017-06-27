package org.pangolin.xuzhe.positiveorder;

import com.alibaba.middleware.race.sync.Server;
import org.pangolin.xuzhe.stringparser.MyStringBuilder;
import org.pangolin.yx.MLog;

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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.pangolin.xuzhe.positiveorder.Constants.*;
import static org.pangolin.xuzhe.positiveorder.ReadBufferPool.EMPTY_BUFFER;

/**
 * Created by ubuntu on 17-6-3.as
 */
final public class Parser extends Thread {

	private BlockingQueue<ByteBuffer> buffers; // 原始数据输入队列
	private BlockingQueue<LogIndex>[] logIndexBlockingQueueArray; // 中间信息输出队列
	private int parserNo;
	private LogIndexPool logIndexPool;
	private Schema schema;
	private int databaseNameLen;
	private int tableNameLen;
	public Parser(int parserNo) {
		this.setName("Parser" + parserNo);
		this.parserNo = parserNo;
		this.buffers = new LinkedBlockingQueue<>(PARSER_BLOCKING_QUEUE_SIZE);
		logIndexBlockingQueueArray = new LinkedBlockingQueue[REDO_NUM];
		for(int i = 0; i < REDO_NUM; i++) {
			logIndexBlockingQueueArray[i] = new LinkedBlockingQueue<>(PARSER_BLOCKING_QUEUE_SIZE);
		}
	}

	/**
	 * 由Redo线程调用，获取中间结果
	 * @param redoId
	 * @return
	 * @throws InterruptedException
	 */
	public LogIndex getLogIndexQueueHeaderByRedoId(int redoId) throws InterruptedException {
		LogIndex index =  logIndexBlockingQueueArray[redoId].take();
		return index;
	}

	/**
	 * 由ReadingThread调用，将分配到该Parser线程的原始数据加入输入队列
	 * @param buffer
	 * @throws InterruptedException
	 */
	public void appendBuffer(ByteBuffer buffer) throws InterruptedException {
		this.buffers.put(buffer);
	}

	@Override
	public void run() {
		try {
			ReadingThread.parserLatch.await();
			logIndexPool = LogIndexPool.getInstance();
			Schema schema = Schema.getInstance();
			databaseNameLen = schema.databaseNameLen;
			tableNameLen = schema.tableNameLen;
			while(true) {
				ByteBuffer buffer = this.buffers.take();
				if(buffer == EMPTY_BUFFER) {
					for(int r = 0; r < REDO_NUM; r++) {
						logIndexBlockingQueueArray[r].put(LogIndex.EMPTY_LOG_INDEX);
					}
					break;
				}
				process(buffer);
			}
			MLog.info("{} done!"+Thread.currentThread().getName());
		} catch (InterruptedException e) {
			MLog.info("Worker was interrupted"+e.toString());
		} catch (Exception e) {
			MLog.info("{}"+ e.toString());
		}

	}

	private void process(ByteBuffer buffer) throws InterruptedException {
		int itemIndex = 0;
		int dataBegin = buffer.position();
		int dataEnd = buffer.limit();
		int lineEnd;
		byte[] data = buffer.array();
		int op = 0;
		long oldPK, newPK;
		int hash; //通过计算每个列名的hash，获取列序号
		int i;
		int b;
		LogIndex logIndex = logIndexPool.get();
		logIndex.setByteBuffer(buffer);

		int logItemIndex = 0;
		// 以 |mysql-bin.00001717148769|1496736165000|middleware3|student|I|id:1:1|NULL|11
		// |first_name:2:0|NULL|阮|last_name:2:0|NULL|甲|sex:2:0|NULL|女|score:1:0|NULL|53|
		for (i = dataBegin; i < dataEnd; ++i) {
			b = data[i];
			if (b == '|') {
				++itemIndex;
				if(itemIndex == 1) {
					i += 18;
				} else if(itemIndex == 2) {
					i += (13 + 2 + 1 + 1 + databaseNameLen + tableNameLen); // 将固定知道长度的item跳过
					itemIndex = 5;
					op = data[i];//I|id:1:1|NULL|11|first_na...

					if (op != 'I' && op != 'U' && op != 'D')
						throw new RuntimeException("op error:" + (char) op);
					// 直接跳过  id:1:1
					i += 9; // after:  NULL|11|first_na..
					if (op == 'I') {
						oldPK = -1;
						i += 5;  // after: 11|first_name:2:0|NULL|阮
					} else {
						oldPK = 0;
						while ((b = data[i]) != '|') {
							oldPK = oldPK * 10 + (b - '0');
							++i;
						}
						++i;
					}
					if(op == 'D') {
						newPK = -1;
						i += 5;
					} else {
						newPK = 0;
						while ((b = data[i]) != '|') {
							newPK = newPK * 10 + (b - '0');
							++i;
						}
					}
					--i; // after:
					itemIndex += 3;
					logIndex.addNewLog(oldPK, newPK, op, logItemIndex);
				} else if (itemIndex == 9) {
					if(op == 'D') {
						while(data[i] != '\n') {
							++i;
						}
						itemIndex = 0;
						++logItemIndex;
					} else {
						// current: |first_name:2:0|NULL|阮|...
						// 除PK以外，第一列的开始
						int[] hashs = logIndex.getHashColumnName(logItemIndex);
						int[] columnNewValues = logIndex.getColumnNewValues(logItemIndex);
						int[] columnValueLens = logIndex.getColumnValueLens(logItemIndex);
						int columnIndex = 0;  // 记录有几个列的值要被更新
						while (data[i+1] != '\n') {
							// 计算column name的hash code
							++i;
							hash = 0;
							while ((b = data[i]) != ':') {
								hash = 31 * hash + b;
								++i;
							} // after: :2:0|NULL|阮|...
							hashs[columnIndex] = schema.columnHash2NoMap.get(hash);  // 获取列名的序号

							i += 4; // after: |NULL|阮|last_name:2:0...
							// 开始解析列的新旧值
							if (op == 'I') {
								i += 5; // 插入操作的旧值为null，直接跳过
										// after: |阮|last_name:2:0...
							} else {
								++i;
								while ((b = data[i]) != '|') {
									++i;
								}
							}
							++i; // after: 阮|last_name:2:0...
							int newValueBegin = i;
							while ((b = data[i]) != '|') {
								++i;
							}
							int newValueEnd = i;
							columnNewValues[columnIndex] = newValueBegin;
							columnValueLens[columnIndex] = newValueEnd - newValueBegin;
							++columnIndex;
						}
						logIndex.setColumnSize(logItemIndex, columnIndex);
					}
				} else {

				}
			} else if (b == '\n') {
				// 处理完一行了
				itemIndex = 0;
				++logItemIndex;
			}
		}
		logIndex.setLogSize(logItemIndex);
		for(int j = 0; j < REDO_NUM; j++) {
//			logIndex.release();
			logIndexBlockingQueueArray[j].put(logIndex);
		}
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
