package org.pangolin.xuzhe.positiveorder;

import com.alibaba.middleware.race.sync.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.pangolin.xuzhe.positiveorder.Parser;
import org.pangolin.yx.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

import static org.pangolin.xuzhe.positiveorder.Constants.LINE_MAX_LENGTH;
import static org.pangolin.xuzhe.positiveorder.Constants.PARSER_NUM;
import static org.pangolin.xuzhe.positiveorder.Constants.REDO_NUM;
import static org.pangolin.xuzhe.positiveorder.ReadBufferPool.EMPTY_BUFFER;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ReadingThread extends Thread {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    public static long beginId = 0;
    public static long endId = 0;

    String[] fileNameArray;
    Parser[] parsers;
    Redo[] redos;
    Schema schema;
    public static final CountDownLatch parserLatch = new CountDownLatch(1);
//    Filter filter;
    public ReadingThread(String[] fileNameArray) {
        super("ReadingThread");
        this.fileNameArray = fileNameArray;
        parsers = new Parser[PARSER_NUM];
        redos = new Redo[REDO_NUM];
        for(int i = 0; i < PARSER_NUM; i++) {
            parsers[i] = new Parser(i);
        }
        for(int i = 0;  i < REDO_NUM; i++){
            redos[i] = new Redo(i, parsers);
        }
        for(int i = 0; i < PARSER_NUM; i++) {
            parsers[i].start();
        }
        for(int i = 0; i < REDO_NUM; i++) {
            redos[i].start();
        }
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
                        LogIndexPool.setColumnCount(schema.columnCount);
                        for(int p = 0; p < PARSER_NUM; ++p) {
                            parsers[p].setSchema(schema);
                        }
                        ReadingThread.parserLatch.countDown();
                        firstRead = false;
                    }
//                    firstLineTest(currentBuffer);
                    remain = remain-lastReadCnt;
                    nextBuffer = pool.get();
//                    if(nextBuffer != null) {
                    transferLastBrokenLine(currentBuffer, nextBuffer);
//                    }
//                    pool.put(currentBuffer);
                    int w = workerIndex% PARSER_NUM;
                    parsers[w].appendBuffer(currentBuffer);
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
            endTime = System.currentTimeMillis();
            logger.info("Parser Done!" + (endTime-beginTime) + " ms");
            for(Redo redo : redos) {
                redo.join();
            }
            endTime = System.currentTimeMillis();
            logger.info("Redo Done!" + (endTime-beginTime) + " ms");
            int totalLine = 0;
            int totalReadBytes = 0;
            for(int i = 0; i < PARSER_NUM; i++) {
                totalLine += parsers[i].readLineCnt;
                totalReadBytes += parsers[i].readBytesCnt;
            }


            endTime = System.currentTimeMillis();
            //  readingThread.sleep(3000);
            logger.info("TOTALLINE:" + totalLine);
            logger.info("TOTALByte:" + totalReadBytes);
            logger.info("elapsed time:" + (endTime - beginTime));
//            if(totalLine != 108796978) {
            logger.info("read line count " + totalLine);
//            }
            ByteBuf buf = Unpooled.directBuffer(20<<20);

            beginTime = System.currentTimeMillis();
//            this.saveResultToFile("Result.rs", beginId, endId);
            saveResultToByteBuf(buf, beginId, endId);
            ResultSenderHandler.sendResult(buf);
            endTime = System.currentTimeMillis();
            logger.info("Send to client elapsed time: " + (endTime-beginTime));
//            this.searchResult();
        } catch (IOException e) {
            logger.info("{}", e);
        } catch (InterruptedException e) {
            logger.info("{}", e);
        }
    }

    private void firstLineTest(ByteBuffer src) {
        int i;
        for(i = 0; i < LINE_MAX_LENGTH; ++i) {
            byte b = src.get(i);
            if(b == '\n') {
                break;
            }
        }
        byte[] bytes = new byte[i+1];
        int pos = src.position();
        src.position(0);
        src.get(bytes);
        String s = new String(bytes);
        System.out.println("firstLine :" + s);
    }

    private void lastBrokenLineTest(ByteBuffer src, int length, int pos) {
        byte[] bytes = new byte[length];
        src.position(pos);
        src.get(bytes);
        String s = new String(bytes);
        System.out.println("brokenLine:" + s);
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

    public void saveResultToFile(String fileName, long begin, long end) throws IOException {
        long beginTime = System.currentTimeMillis();
        BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(fileName));
        byte[] buf = new byte[1024];
        byte[] pkStrBuf = new byte[64];
        for(begin++; begin < end; begin++) {
            for(int i = 0; i < REDO_NUM; i++) {
                long pk = begin;
                int len = redos[i].getRecord(pk,  buf, 0);
                if(len != -1) {
                    {
                        int pkStrPos = pkStrBuf.length;
                        while (pk > 0) {
                            --pkStrPos;
                            pkStrBuf[pkStrPos] = (byte)(pk%10+'0');
                            pk /= 10;
                        }
                        writer.write(pkStrBuf, pkStrPos, pkStrBuf.length-pkStrPos);
                    }
                    writer.write(buf, 0, len);
                }
//                Record record = redos[i].pkMap.get(begin);
//
//                if (record != null) {
//                    writer.write(record.toString());
//                    writer.write('\n');
//                }
            }
        }
        writer.close();
        long endTime = System.currentTimeMillis();
        System.out.println("save to file elapsed time:" + (endTime-beginTime));
    }

    public void saveResultToByteBuf(ByteBuf buf, long begin, long end) {
//        buf.writeChar('R');
        long beginTime = System.currentTimeMillis();
        buf.writeInt(0);
        byte[] recordBuf = new byte[1024];
        byte[] pkStrBuf = new byte[64];
        for (begin++; begin < end; begin++) {
            for (int i = 0; i < REDO_NUM; i++) {
                long pk = begin;
                int len = redos[i].getRecord(pk, recordBuf, 0);
                if (len != -1) {
                    {
                        int pkStrPos = pkStrBuf.length;
                        while (pk > 0) {
                            --pkStrPos;
                            pkStrBuf[pkStrPos] = (byte) (pk % 10 + '0');
                            pk /= 10;
                        }
                        buf.writeBytes(pkStrBuf, pkStrPos, pkStrBuf.length - pkStrPos);
                    }
                    buf.writeBytes(recordBuf, 0, len);
                }


            }

        }
        int len = buf.readableBytes();
        buf.writerIndex(0);
        buf.writeInt(len - 4);
        buf.writerIndex(len);
        long endTime = System.currentTimeMillis();
        logger.info("save to buf elapsed time:{}", (endTime - beginTime));
    }

    public void searchResult() {
        System.out.println("开始测试索引信息，请输入主键（quit退出）：");
        Scanner scanner = new Scanner(System.in);
        Redo[] redos = this.redos;
        while (scanner.hasNext()) {
            String line = scanner.nextLine().trim();
            if (line.startsWith("quit")) break;
            try {
                Long pk = Long.valueOf(line);
                for (int i = 0; i < REDO_NUM; i++) {
//                    Record record = redos[i].pkMap.get(pk);
//
//                    if (record != null) {
//                        System.out.println(record);
//                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("请输入查询范围");
            System.exit(-1);
        }
        ReadingThread.beginId = Long.parseLong(args[0]);
        ReadingThread.endId = Long.parseLong(args[1]);
        Config.init();
        String fileBaseName = Config.DATA_HOME + "/";
//        String fileBaseName = Config.DATA_HOME + "/small_";
//        String fileBaseName = "G:/研究生/AliCompetition/quarter-final/home/data/";
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

