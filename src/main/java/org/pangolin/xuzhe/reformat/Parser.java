package org.pangolin.xuzhe.reformat;

import com.alibaba.middleware.race.sync.Server;
import org.pangolin.yx.MLog;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.Deflater;

import static org.pangolin.xuzhe.reformat.Constants.*;
import static org.pangolin.xuzhe.reformat.ReadBufferPool.EMPTY_BUFFER;


/**
 * Created by ubuntu on 17-6-3.as
 */
public class Parser extends Thread {
	public BlockingQueue<ByteBuffer> buffers;
	private int parserNo;
	public int readLineCnt = 0;
	public long readBytesCnt = 0;
	private Schema schema;
	private int databaseNameLen;
	private int tableNameLen;
	private long totalSize = 0;
	public BlockingQueue<byte[]> compressedBytesBlockingQueue;
	public BlockingQueue<ByteArrayPool.ByteArray>[] blockingQueue = new LinkedBlockingQueue[REDO_NUM];
	public Parser(int parserNo) {
		this.setName("Parser" + parserNo);
		this.parserNo = parserNo;
		this.buffers = new ArrayBlockingQueue<ByteBuffer>(PARSER_BLOCKING_QUEUE_SIZE);
		compressedBytesBlockingQueue = new ArrayBlockingQueue<byte[]>(PARSER_BLOCKING_QUEUE_SIZE);
		for(int i = 0; i < REDO_NUM; i++) {
			blockingQueue[i] = new LinkedBlockingQueue<>(PARSER_BLOCKING_QUEUE_SIZE);
		}
	}

	public void appendBuffer(ByteBuffer buffer) throws InterruptedException {
		this.buffers.put(buffer);
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public long getTotalSize() {
		return totalSize;
	}

	@Override
	public void run() {
		ReadBufferPool pool = ReadBufferPool.getInstance();
		ByteArrayPool byteArrayPool = ByteArrayPool.getInstance();
//		java.util.zip.Deflater def = new java.util.zip.Deflater(1);
		try {
//			byte[] out = new byte[Constants.BUFFER_SIZE];
			byte[] result = new byte[1024*1024];
			ReadingThread.parserLatch.await();
			Schema schema = Schema.getInstance();
			databaseNameLen = schema.databaseNameLen;
			tableNameLen = schema.tableNameLen;

			int cnt = 0;
			while(true) {
				ByteBuffer buffer = null;
				try {
					buffer = this.buffers.take();
					if (buffer == EMPTY_BUFFER) {
//					compressedBytesBlockingQueue.put(new byte[0]);
						for (int i = 0; i < REDO_NUM; i++) {
							blockingQueue[i].put(ByteArrayPool.EMPTY_ARRAY);
						}
						break;
					}
					long begin = System.nanoTime();

					ByteArrayPool.ByteArray out = byteArrayPool.get();
					int size = process(buffer, out.array, 0, out.array.length);
					pool.put(buffer);
//				System.out.println(getName() + " processed a buffer");
					int no = cnt*PARSER_NUM + this.parserNo;
					++cnt;
					out.no = no;
					for (int i = 0; i < REDO_NUM; i++) {
						out.dataSize = size;
//					out.release();
						blockingQueue[i].put(out);
					}
//				int compressedSize = compress(def, out, 0, size, result, 0, result.length);
//				byte[] result1 = Arrays.copyOf(result, compressedSize);
//				int compressedSize = compress.snappy.SnappyRawCompressor.compress( out, 0, size, result, 0, result.length);
//				byte[] result1 = Arrays.copyOf(result, compressedSize);
//				byte[] newBuffer = new byte[size];
//				Redo.uncompress(result, 0, compressedSize, newBuffer, 0, size);
//				boolean eq = Arrays.equals(Arrays.copyOf(out, size), newBuffer);
//				compressedBytesBlockingQueue.put(result1);
//				totalSize += result.length;
//				totalSize += result1.length;
				} finally {

					long end = System.nanoTime();
//					System.out.println("Parser " + this.parserNo + " release a buffer");
				}
			}
		} catch (InterruptedException e) {
			MLog.info(""+ e);
		} catch (Exception e) {
			MLog.info(""+ e);
		}

	}

	private int process(ByteBuffer buffer, byte[] out, int offsetOut, int limitOut) throws InterruptedException {
		int itemIndex = 0;
		int dataBegin = buffer.position();
		int dataEnd = buffer.limit();
		int lineBegin = buffer.position();
		int lineEnd = -1;
		int subBegin = -1;
		byte[] data = buffer.array();
		int op = 0;
		long oldPK = -1, newPK = -1;
		int i = 0;
		int b;
		int logItemIndex = 0;
		int databaseNameLen = 11;
		int tableNameLen = 7;
//		int offsetOutBegin = offsetOut;
//		offsetOut += 4;
		// 以 |mysql-bin.00001717148769|1496736165000|middleware3|student|I|id:1:1|NULL|11
		// |first_name:2:0|NULL|阮|last_name:2:0|NULL|甲|sex:2:0|NULL|女|score:1:0|NULL|53|
		for (i = dataBegin; i < dataEnd; ++i) {
			b = data[i];
			if (b == '|') {
				++itemIndex;
				if(itemIndex == 1) {
					i += 18;
				} else if(itemIndex == 2) {
					i += (13 + 1 + 1 + databaseNameLen + tableNameLen);
					itemIndex = 4;
				} else if (itemIndex == 5) {
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
					--i; // after:
					itemIndex += 3;
					out[offsetOut] = (byte)op;
                    if(op == 'I') {
						// oldPK
						out[offsetOut + 1] = (byte) (newPK >> 56);
						out[offsetOut + 2] = (byte) (newPK >> 48);
						out[offsetOut + 3] = (byte) (newPK >> 40);
						out[offsetOut + 4] = (byte) (newPK >> 32);
						out[offsetOut + 5] = (byte) (newPK >> 24);
						out[offsetOut + 6] = (byte) (newPK >> 16);
						out[offsetOut + 7] = (byte) (newPK >> 8);
						out[offsetOut + 8] = (byte) (newPK);
						offsetOut += 9;
					} else if(op == 'U') {
						if(newPK == oldPK) {
							out[offsetOut + 1] = (byte) 0; // 记录是否主键变更
							out[offsetOut + 2] = (byte) (oldPK >> 56);
							out[offsetOut + 3] = (byte) (oldPK >> 48);
							out[offsetOut + 4] = (byte) (oldPK >> 40);
							out[offsetOut + 5] = (byte) (oldPK >> 32);
							out[offsetOut + 6] = (byte) (oldPK >> 24);
							out[offsetOut + 7] = (byte) (oldPK >> 16);
							out[offsetOut + 8] = (byte) (oldPK >> 8);
							out[offsetOut + 9] = (byte) (oldPK);
							offsetOut += 10;
						} else {
							out[offsetOut + 1] = (byte) 1; // 记录是否主键变更
							out[offsetOut + 2] = (byte) (oldPK >> 56);
							out[offsetOut + 3] = (byte) (oldPK >> 48);
							out[offsetOut + 4] = (byte) (oldPK >> 40);
							out[offsetOut + 5] = (byte) (oldPK >> 32);
							out[offsetOut + 6] = (byte) (oldPK >> 24);
							out[offsetOut + 7] = (byte) (oldPK >> 16);
							out[offsetOut + 8] = (byte) (oldPK >> 8);
							out[offsetOut + 9] = (byte) (oldPK);
							out[offsetOut + 10] = (byte) (newPK >> 56);
							out[offsetOut + 11] = (byte) (newPK >> 48);
							out[offsetOut + 12] = (byte) (newPK >> 40);
							out[offsetOut + 13] = (byte) (newPK >> 32);
							out[offsetOut + 14] = (byte) (newPK >> 24);
							out[offsetOut + 15] = (byte) (newPK >> 16);
							out[offsetOut + 16] = (byte) (newPK >> 8);
							out[offsetOut + 17] = (byte) (newPK);
							offsetOut += 18;
						}
					} else {
						// 删除操作只记录旧键
						out[offsetOut + 1] = (byte) (oldPK >> 56);
						out[offsetOut + 2] = (byte) (oldPK >> 48);
						out[offsetOut + 3] = (byte) (oldPK >> 40);
						out[offsetOut + 4] = (byte) (oldPK >> 32);
						out[offsetOut + 5] = (byte) (oldPK >> 24);
						out[offsetOut + 6] = (byte) (oldPK >> 16);
						out[offsetOut + 7] = (byte) (oldPK >> 8);
						out[offsetOut + 8] = (byte) (oldPK);
						offsetOut += 9;
					}

				} else if (itemIndex == 9) {
					// current: |first_name:2:0|NULL|阮|...
					// 除PK以外，第一列的开始
//					buffer.position(i);
//					printSubLine(buffer);
					int columnCountIndexInOut = offsetOut;
					if(op == 'U')
						++offsetOut; // 暂时跳过存放columnCount的位置
					int columnIndex = 0;
					while(data[i+1] != '\n') {
						// 计算column name的hash code
						++i;
						int hash = 0;
						while ((b = data[i]) != ':') {
							hash = 31 * hash + b;
//						System.out.print((char)b);
							++i;
						} // after: :2:0|NULL|阮|...
//					System.out.println();

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
						if(op != 'D') {
							int offMark = offsetOut;
							if(op == 'U') {
								out[offMark++] = (byte) schema.columnHash2NoMap.get(hash);
							}
							int len = (newValueEnd - newValueBegin);
							out[offMark++] = (byte) len;
							for (int j = newValueBegin; j < newValueEnd; j++) {
								out[offMark++] = data[j];
							}
							offsetOut += 8;
						}
						++columnIndex;
					}
					if(op == 'U') {
						out[columnCountIndexInOut] = (byte) columnIndex;
					}
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
//		int len = offsetOut-4;
//		out[offsetOutBegin]   = (byte)(len >> 24);
//		out[offsetOutBegin+1] = (byte)(len >> 16);
//		out[offsetOutBegin+2] = (byte)(len >>  8);
//		out[offsetOutBegin+3] = (byte)(len      );
		return offsetOut;
	}

	public static int compress(Deflater def, final byte[] src, int off, int lenIn, byte[] output, int offOut, int limitOut) {
		int outOffset = 0;
		def.reset();
		def.setInput(src, off, lenIn);
		def.finish();
		while (!def.finished()) {
			outOffset += def.deflate(output, offOut+outOffset, limitOut-offOut-outOffset);
		}
		return outOffset;
	}
}
