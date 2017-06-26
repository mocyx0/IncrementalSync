package org.pangolin.xuzhe.reformat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledDirectByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static org.pangolin.xuzhe.reformat.Constants.PARSER_NUM;
import static org.pangolin.xuzhe.reformat.Constants.REDO_NUM;
import static org.pangolin.xuzhe.reformat.ReadBufferPool.EMPTY_BUFFER;
import static org.pangolin.xuzhe.reformat.ResultSenderHandler.latch;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ReadingThread extends Thread {
    Logger logger = LoggerFactory.getLogger(ReadingThread.class);
    public static int beginId = 0;
    public static int endId = 0;

    String[] fileNameArray;
    Parser[] parsers;
    Schema schema;
    Redo[] redos;
//    WriterThread writerThread;
    public static final CountDownLatch parserLatch = new CountDownLatch(1);
//    Filter filter;
    public ReadingThread(String[] fileNameArray) {
        super("ReadingThread");
        this.fileNameArray = fileNameArray;
        parsers = new Parser[PARSER_NUM];
        for(int i = 0; i < PARSER_NUM; i++) {
            parsers[i] = new Parser(i);

        }
        for(int i = 0; i < PARSER_NUM; i++) {
            parsers[i].start();
        }
        redos = new Redo[REDO_NUM];
        for(int i = 0; i< REDO_NUM; i++) {
            redos[i] = new Redo(i, parsers);
            redos[i].start();
        }
//        writerThread = new WriterThread(parsers);
//        writerThread.start();
//        filter = new Filter();
//        filter.start();

    }

    @Override
    public void run() {
        ReadBufferPool pool = ReadBufferPool.getInstance();
        int workerIndex = 0;
        FileInputStream fis;
        try {
            long beginTime = System.currentTimeMillis();
            ByteBuffer currentBuffer, nextBuffer = null;
            boolean firstRead = true;
            currentBuffer = pool.get();
            for (String fileName : fileNameArray) {
                logger.info(fileName);
                fis = new FileInputStream(new File(fileName));
                FileChannel channel = fis.getChannel();
                long fileSize = channel.size();
                int lastReadCnt;
                int remain = (int)fileSize;
                while(true) {
                    lastReadCnt = channel.read(currentBuffer);
                    currentBuffer.flip();
                    if(firstRead) {
                        byte[] data = currentBuffer.array();
                        for(int i = 0; i < data.length; i++) {
                            if(data[i] == '\n') {
                                schema = Schema.generateFromInsertLog(Arrays.copyOf(data, i+1));
                                break;
                            }
                        }
                        if(schema == null) {
                            throw new RuntimeException("严重错误，Schema未解析成功！");
                        }
                        for(int p = 0; p < PARSER_NUM; ++p) {
                            parsers[p].setSchema(schema);
                        }
                        ReadingThread.parserLatch.countDown();
                        firstRead = false;
                    }
                    remain = remain-lastReadCnt;
                    nextBuffer = pool.get();
                    transferLastBrokenLine(currentBuffer, nextBuffer);
                    int w = workerIndex % PARSER_NUM;
//                    System.out.println("parser " + w + " put a buffer");
                    parsers[w].appendBuffer(currentBuffer);
//                    for(int i = 0; i < PARSER_NUM; i++) {
//                        System.out.println("PARSER" + i + ".buffers size:" + parsers[i].buffers.size());
//                    }
//                    pool.put(currentBuffer);
                    workerIndex++;
                    currentBuffer = nextBuffer;
                    if(remain == 0) {
                        break;
                    }
                }
                channel.close();
                fis.close();
            }
            long endTime = System.currentTimeMillis();
            logger.info("Reading Done! " + (endTime-beginTime) + " ms");
            for(Parser parser : parsers) {
                parser.appendBuffer(EMPTY_BUFFER);
            }
            for(Parser parser : parsers) {
                parser.join();
            }
//            writerThread.join();
            for(Redo redo : redos) {
                redo.join();
            }
            endTime = System.currentTimeMillis();
            logger.info("Redo Done! " + (endTime-beginTime) + " ms");
            int totalLine = 0;
            long totalReadBytes = 0;
            long totalSize = 0;
            for(int i = 0; i < PARSER_NUM; i++) {
                totalLine += parsers[i].readLineCnt;
                totalReadBytes += parsers[i].readBytesCnt;
                totalSize += parsers[i].getTotalSize();
            }
            logger.info("TotalSize:"+totalSize);
            long t1 = System.currentTimeMillis();
            FileOutputStream outputStream = new FileOutputStream("Result.rs");

            latch.countDown();
            byte[] result = new byte[50_000_000];
            final int blockSize = 5000_000;
            int offset = 0;
            for(int i = beginId+1; i < endId; i++) {
                if(offset > blockSize) {
                    ByteBuf nettyBuf = Unpooled.directBuffer(blockSize+1000);
                    nettyBuf.writeInt(offset);
                    nettyBuf.writeBytes(result, 0, offset);
                    ResultSenderHandler.resultQueue.put(nettyBuf);
                    outputStream.write(result, 0, offset);
                    offset = 0;
                }
                for(int j = 0; j < redos.length; j++) {
                    int tmp = redos[j].getRecord(i, result, offset);

                    if (tmp != -1) {
                        offset = tmp;
                        break;
                    }
                }
            }
            if(offset != 0) {
                ByteBuf nettyBuf = Unpooled.directBuffer(blockSize+1000);
                nettyBuf.writeInt(offset);
                nettyBuf.writeBytes(result, 0, offset);
                ResultSenderHandler.resultQueue.put(nettyBuf);
                ResultSenderHandler.resultQueue.put(Unpooled.directBuffer(0));
                outputStream.write(result, 0, offset);
                offset = 0;
            }
            outputStream.close();
            long t2 = System.currentTimeMillis();
//            logger.info(new String(result, 0, 200));
            logger.info("cnt:{}  time:{} ms", offset, t2-t1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private int transferLastBrokenLine(ByteBuffer src, ByteBuffer dst) {
        int currentPos = src.position();
        int currentLimit = src.limit();
        int i;
        for(i = currentLimit-1; i >= currentPos; --i) {
            byte b = src.get(i);
            if(b == '\n') {
                break;
            }
        }
//        lastBrokenLineTest(src, currentLimit-i-1, i+1);
        src.position(i+1);
        dst.put(src);
        src.position(0); // src.flip
        src.limit(i+1);
        return currentLimit - i;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("请输入查询范围");
            System.exit(-1);
        }
        ReadingThread.beginId = Integer.parseInt(args[0]);
        ReadingThread.endId = Integer.parseInt(args[1]);
        String fileBaseName = "D:\\Code\\java\\中间件复赛\\test_data\\Downloads\\";
//        String fileBaseName = "/root/ram/";
        int fileCnt = 0;
        for (int i = 1; i <= 10; i++) {
            String fileName = fileBaseName + i + ".txt";
            System.out.print("check file:" + fileName);
            File f = new File(fileName);
            if (f.exists()) {
                System.out.println(" exists");
                fileCnt++;
            } else {
                System.out.println(" not exists");

            }
        }
        String[] fileNames = new String[fileCnt];
        for (int i = 1; i <= fileCnt; i++) {
            fileNames[i - 1] = fileBaseName + i + ".txt";
        }
        long time1 = System.currentTimeMillis();
        ReadingThread readingThread = new ReadingThread(fileNames);
        readingThread.start();
        readingThread.join();
        long time2 = System.currentTimeMillis();
        System.out.println("elapsed time:" + (time2 - time1) + "ms");
    }

}

