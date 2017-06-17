package org.pangolin.xuzhe.positiveorder;

import org.pangolin.xuzhe.positiveorder.Parser;
import org.pangolin.yx.Config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static org.pangolin.xuzhe.positiveorder.Constants.LINE_MAX_LENGTH;
import static org.pangolin.xuzhe.positiveorder.Constants.PARSER_NUM;
import static org.pangolin.xuzhe.positiveorder.Constants.REDO_NUM;
import static org.pangolin.xuzhe.positiveorder.ReadBufferPool.EMPTY_BUFFER;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ReadingThread extends Thread {
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
            redos[i] = new Redo(parsers, 1, 100);
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
                    int w = workerIndex% PARSER_NUM;
                    nextBuffer = pool.get();
//                    if(nextBuffer != null) {
                        transferLastBrokenLine(currentBuffer, nextBuffer);
//                    }
//                    pool.put(currentBuffer);
                    parsers[w].appendBuffer(currentBuffer);
                    currentBuffer = nextBuffer;
                    if(remain == 0) {
                        break;
                    }
                    workerIndex++;
                }
                channel.close();
                fis.close();
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Reading Done! " + (endTime-beginTime) + " ms");
            for(Parser parser : parsers) {
                parser.appendBuffer(EMPTY_BUFFER);
            }
            for(Parser parser : parsers) {
                parser.join();
            }
            int totalLine = 0;
            int totalReadBytes = 0;
            for(int i = 0; i < PARSER_NUM; i++) {
                totalLine += parsers[i].readLineCnt;
                totalReadBytes += parsers[i].readBytesCnt;
            }


            endTime = System.currentTimeMillis();
            //  readingThread.sleep(3000);
            System.out.println("TOTALLINE:" + totalLine);
            System.out.println("TOTALByte:" + totalReadBytes);
            System.out.println("elapsed time:" + (endTime - beginTime));
            if(totalLine != 14787781) {
                System.out.println("read line count error!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
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


    public static void main(String[] args) throws Exception {
        Config.init();
//        String fileBaseName = Config.DATA_HOME + "/small_";
        String fileBaseName = "G:/研究生/AliCompetition/quarter-final/home/data/";
        int fileCnt = 0;
        for(int i = 1; i <= 10; i++) {
            String fileName = fileBaseName + i + ".txt";
            File f = new File(fileName);
            if(f.exists()) fileCnt++;
        }
        String[] fileNames = new String[fileCnt];
        for(int i = 1; i <= fileCnt; i++) {
            fileNames[i-1] = fileBaseName + i + ".txt";
        }
        long time1 = System.currentTimeMillis();
        ReadingThread readingThread = new ReadingThread(fileNames);
        readingThread.start();
        readingThread.join();
        long time2 = System.currentTimeMillis();
        System.out.println("elapsed time:" + (time2 - time1));
    }
}
