package org.pangolin.xuzhe.stringparser;

import org.pangolin.yx.Config;
import org.pangolin.yx.MLog;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by ubuntu on 17-6-3.
 */
public class Worker extends Thread {
    private final ArrayList<String> localItemsBuffer = new ArrayList<>();
    private static AtomicInteger workerNum = new AtomicInteger(0);
    private int workerNo = workerNum.getAndIncrement();
    private MyStringBuilder lineBuilder = new MyStringBuilder(200);
    private static ReentrantLock readLock = new ReentrantLock();

    public Worker() {
        this.setName("Worker" + workerNo);
    }

    @Override
    public void run() {

        byte[] buffer = new byte[20 * (1 << 20)];
//		byte[] buffer = new byte[240];
        final byte newLine = (byte) '\n';
        try {
            int fileNo = 1;
            while (true) {
                File f = new File(Constants.getFileNameByNo(fileNo));
                if (!f.exists()) break;
                System.out.println("reading " + f.getName());
                RandomAccessFile raf = new RandomAccessFile(f, "r");
                int fileSize = (int) raf.length();
                int blockSize = (int) Math.round(((double) fileSize) / Constants.WORKER_NUM);
                int begin = blockSize * workerNo;
                int currentPos = begin;
                int end = Math.min(blockSize * (workerNo + 1), fileSize); //不读end这个位置的数据
                raf.seek(begin);
                int readCnt = (end - begin);
                boolean firstRead = true;
                if (workerNo == 2) {
                    System.out.println();
                }
                for (int i = 0; i < readCnt; ) {
//					System.out.println(String.format("%s : read pos:%d %d(%d,%d)", getName(), fileNo, begin+i, begin, end));
                    int n = 0, cnt = 0;
                    long beginTime = System.nanoTime();
                    int needRead = Math.min(readCnt - i, buffer.length);
                    readLock.lock();
                    while ((n = raf.read(buffer, cnt, needRead - cnt)) != -1) {
                        cnt += n;
                        if (cnt == needRead) break;
                    }
                    readLock.unlock();
                    i += n;
                    int j = 0;
                    // 判断第一行是不是完整的一行
                    if (firstRead) {
                        if (buffer[0] != '|' || buffer[6] != '-' || buffer[10] != '.') {
                            while (buffer[j] != '\n') {
                                ++j;
                            }
                            ++j; // 跳过换行符
                        }
                        firstRead = false;
//						System.out.println(getName() + "半行内容：" +  new String(buffer, 0, j));
                    }
                    currentPos += j;
//					System.out.println(getName() + "文件初始内容:" + new String(buffer, 0, 150));
                    long endTime = System.nanoTime();
                    for (; j < cnt; j++) {
                        byte b = buffer[j];
                        if (b != newLine) {
                            lineBuilder.append(b);
                        } else {
                            int pos = currentPos;
//							String str = lineBuilder.toString();
//							process(str, fileNo, pos);
                            currentPos += (lineBuilder.getSize() + 1);
                            lineBuilder.clear();
                        }
                    }
                    if (workerNo == 0)
                        n = n;
                }
                if (lineBuilder.getSize() != 0) {
                    raf.seek(end);
                    while (true) {
                        byte b = raf.readByte();
                        if (b != newLine) {
                            lineBuilder.append(b);
                        } else {
                            int pos = currentPos;
                            String str = lineBuilder.toString();
                            process(str, fileNo, pos);
                            currentPos += (lineBuilder.getSize() + 1);
                            lineBuilder.clear();
                            break;
                        }
                    }
                }
                ++fileNo;
            }
            MLog.info("{} done!"+ Thread.currentThread().getName());
        } catch (IOException e) {
            MLog.info("{} "+ e);
        }

    }


    private void process(String line, int fileNo, int position) {
//		LogParser.parseToIndex(line, fileNo, position, localItemsBuffer);
    }

}
