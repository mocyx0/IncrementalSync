package org.pangolin.xuzhe.pipeline;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.pangolin.xuzhe.pipeline.ByteBufferPool.EMPTY_BUFFER;
import static org.pangolin.xuzhe.pipeline.Constants.*;
import static org.pangolin.xuzhe.pipeline.Filter.logStringListQueue;
import static org.pangolin.xuzhe.pipeline.StringArrayListPool.EMPTY_STRING_LIST;

/**
 * Created by ubuntu on 17-6-3.
 */
public class Parser extends Thread {
	Logger logger = LoggerFactory.getLogger(Server.class);

	public static AtomicInteger lineCnt = new AtomicInteger(0);
	private static final byte newLine = (byte)'\n';


	private int workerNo;
	private MappedByteBuffer[] mappedBuffers;
	private MappedByteBuffer currentBuffer;
	private byte[] secondaryBuffer = new byte[LINE_MAX_LENGTH];
	public int readLineCnt = 0;
	public int readByteCnt = 0;
	public Parser(int no, MappedByteBuffer[] mappedBuffers) {
		super("Parser" + no);
		this.workerNo = no;
		this.mappedBuffers = new MappedByteBuffer[mappedBuffers.length];
		for(int i = 0; i < mappedBuffers.length; i++) {
			this.mappedBuffers[i] = (MappedByteBuffer)mappedBuffers[i].duplicate();
		}

	}

	public void appendBuffer(ByteBuffer buffer) throws InterruptedException {

	}

	@Override
	public void run() {
		final int blockSize = BUFFER_SIZE;
		byte[] bytesLocalBuffer = new byte[blockSize];

		try {
			final int workerCnt = WORKER_NUM;
			int blockNo = 0;
			for(int fileNo = mappedBuffers.length; fileNo > 0; fileNo--) {
				currentBuffer = mappedBuffers[fileNo-1];
				int fileSize = currentBuffer.capacity();
				int blockCnt = (int)Math.ceil(((double)fileSize)/blockSize);
				for(int j = blockCnt-1; j >= 0; j--) {
					int no = blockNo%workerCnt;
					if(no == workerNo) {
						int readCnt = blockSize;
						int beginReadPos;
						if(j == 0) {
							beginReadPos = 0;
							readCnt = fileSize%blockSize;
						} else {
							beginReadPos = fileSize-(blockCnt-j)*blockSize;
						}
						currentBuffer.position(beginReadPos);
						currentBuffer.get(bytesLocalBuffer, 0, readCnt);
						process(bytesLocalBuffer, readCnt, beginReadPos);
					}
					blockNo++;
				}
			}
//			while(true) {
//				long begin = System.nanoTime();
//				byte[] data = buffer.array();
//				int limit = buffer.limit();
//				for(int i = limit-1; i>=0; --i) {
//					byte b = data[i];
//					if(b != newLine) {
//						lineBuilder.append(b);
//					} else if(lineBuilder.getSize() == 0) {
//						// 空串，则跳过
//					} else {
//						lineCnt.incrementAndGet();
////						String str = lineBuilder.toString();
////						process(str);
//						lineBuilder.clear();
//					}
//				}
//				long end = System.nanoTime();
//				pool.put(buffer);
//			}
			logger.info("{} done!", Thread.currentThread().getName());

		} catch (Exception e) {
			logger.info("{}", e);
		}
	}

	/**
	 *
	 * @param bytes
	 * @param length
	 * @param beginPos   该块数据在文件中的起始位置
	 */
	private void process(byte[] bytes, int length, int beginPos) {
		int begin = 0;
		int lastLineBreakPos;
		// 前半行跳过，先判断开始是不是半行
		if( (bytes[0] != '|' || bytes[1] != 'm' || bytes[6] != '-' || bytes[10] != '.')) {
			while (bytes[begin] != '\n') {
				++begin;
			}
			lastLineBreakPos = begin;
			begin += 1; // 跳过换行符
		} else {
			lastLineBreakPos = -1;
		}
		for(int currentPos = begin; currentPos < length; ++currentPos) {
			if(bytes[currentPos] == '\n') {
				parserLine(bytes, lastLineBreakPos + 1, currentPos, beginPos + lastLineBreakPos + 1);
				lastLineBreakPos = currentPos;
			}
		}

		// 读入最后半行的内容, 先判断该块的最后一个字符是不是换行符
		if(lastLineBreakPos != length-1) {
			int end = currentBuffer.position();
			int read = Math.min(LINE_MAX_LENGTH, currentBuffer.limit()-end);  // TODO 改为在程序启动时保证BUFFER_SIZE大于LINE_MAX_LENGTH

			int secondaryOffset = length-(lastLineBreakPos+1); // 半行数据长度
			System.arraycopy(bytes, lastLineBreakPos + 1, secondaryBuffer, 0, secondaryOffset);
			currentBuffer.get(bytes, 0, read);
			for(int i = 0; i < read; i++) {
				byte b = bytes[i];
				secondaryBuffer[secondaryOffset] = b;
				++secondaryOffset;
				if(b == '\n') {
					break;
				}
			}
			for(int i = 0; i < secondaryOffset; i++) {
				if(secondaryBuffer[i] == '\n') {
					parserLine(secondaryBuffer, 0, i, beginPos+lastLineBreakPos + 1);
				}
			}
		}
	}


	/**
	 *
	 * @param bytes 从MappedByteBuffer拷贝到本地线程的数据块
	 * @param begin 当前行在bytes中的起始位置
	 * @param end 当前行在bytes中的结束位置，具体为换行符的位置
	 * @param rawBegin 当前行的数据在原始MappedByteBuffer中的起始位置
	 */
	private void parserLine(byte[] bytes, int begin, int end, int rawBegin) {
//		String s = new String(bytes, begin, end-begin);
//		System.out.println(s);
		int i = begin;
		int cnt = 0;
		while (i < end) {
			if (bytes[i] == '|')
				++cnt;
			++i;
			if (cnt == 5) {
				break; // TODO 统计(i-begin)的值,方便后面加速跳过前面几列数据，直接定位到op列的位置
			}
		}


//		byte[] raw = new byte[end - begin];
		byte[] subRaw = new byte[end - i];
		int mark = currentBuffer.position();
//		currentBuffer.position(rawBegin);
//		currentBuffer.get(raw);
		currentBuffer.position(rawBegin+(i-begin));
		currentBuffer.get(subRaw);
		currentBuffer.position(mark);
//		String s = new String(raw);
		String s2 = new String(subRaw);
		System.out.println(s2);
		++readLineCnt;
		readByteCnt += (end-begin+1);
	}
}
