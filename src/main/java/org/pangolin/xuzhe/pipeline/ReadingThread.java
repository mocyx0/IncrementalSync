package org.pangolin.xuzhe.pipeline;

import org.pangolin.yx.Config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static org.pangolin.xuzhe.pipeline.ByteBufferPool.EMPTY_BUFFER;
import static org.pangolin.xuzhe.pipeline.Constants.BUFFER_SIZE;
import static org.pangolin.xuzhe.pipeline.Constants.WORKER_NUM;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ReadingThread extends Thread {
    String[] fileNameArray;
    Parser[] parsers;
    Filter filter;
    public ReadingThread(String[] fileNameArray) {
        super("ReadingThread");
        this.fileNameArray = fileNameArray;
        parsers = new Parser[WORKER_NUM];
        for(int i = 0; i < WORKER_NUM; i++) {
//            parsers[i] = new Parser();
        }
        for(int i = 0; i < WORKER_NUM; i++) {
            parsers[i].start();
        }
//        filter = new Filter();
//        filter.start();

    }

    @Override
    public void run() {
        ByteBufferPool pool = ByteBufferPool.getInstance();
        int workerIndex = 0;
        FileInputStream fis;
        try {
            long beginTime = System.currentTimeMillis();
            for (String fileName : fileNameArray) {
            	fis = new FileInputStream(new File(fileName));
                FileChannel channel = fis.getChannel();
                long fileSize = channel.size();
                long pos = fileSize - BUFFER_SIZE;
                int lastReadCnt;
                int remain = (int)fileSize;
                ByteBuffer buffer;
                while(true) {
                    buffer = pool.get();
                    if(remain < BUFFER_SIZE) {
                        buffer.put((byte)'\n');
                        buffer.limit(remain+1);
                    }
                    lastReadCnt = channel.read(buffer, pos);
                    buffer.flip();
                    byte[] bytes = buffer.array();
                    int beginPos = 0;
                    int limitPos = buffer.limit();
                    if( bytes[0] != '\n' && (bytes[0] != (byte)'|' || bytes[1] != (byte)'m' || bytes[6] != (byte)'-' || bytes[10] != (byte)'.')) {
                        for(beginPos = buffer.position(); beginPos < limitPos; ++beginPos ) {
                            if(bytes[beginPos] == '\n') {
                                break;
                            }
                        }
                        buffer.position(beginPos);
                    }
                    lastReadCnt -= beginPos;
                    remain = remain-lastReadCnt;
                    pos =  Math.max(pos-lastReadCnt, 0);
                    int w = workerIndex%WORKER_NUM;
                    pool.put(buffer);
//                    parsers[w].appendBuffer(buffer);
                    if(remain == 0) {
                        break;
                    }
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
            endTime = System.currentTimeMillis();
            System.out.println("Parser Done!" + (endTime-beginTime) + " ms");
            System.out.println("Parser read line : " + Parser.lineCnt.get());
//            filter.join();
            endTime = System.currentTimeMillis();
            System.out.println("Filter Done!" + (endTime-beginTime) + " ms");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        Config.init();
//        String fileBaseName = Config.DATA_HOME + "/small_";
        String fileBaseName = Config.DATA_HOME + "/ram/";
        int fileCnt = 0;
        for(int i = 1; i <= 10; i++) {
            String fileName = fileBaseName + i + ".txt";
            File f = new File(fileName);
            if(f.exists()) fileCnt++;
        }
        long time1 = System.currentTimeMillis();
        MappedByteBuffer[] buffers = new MappedByteBuffer[fileCnt];
        for(int i = 1; i <= fileCnt; i++) {
            String fileName = fileBaseName + i + ".txt";
            RandomAccessFile raf = new RandomAccessFile(fileName, "r");
            long size = raf.length();
            MappedByteBuffer buffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, size);
            raf.close();
            buffers[i-1] = buffer;
        }
        Parser[] parsers = new Parser[WORKER_NUM];
        for(int i = 0; i < WORKER_NUM; i++) {
            parsers[i] = new Parser(i, buffers);
        }
        for(int i = 0; i < WORKER_NUM; i++) {
            parsers[i].start();
        }
        for(int i = 0; i < WORKER_NUM; i++) {
            parsers[i].join();
        }

        int totalLine = 0;
        int totalReadBytes = 0;
        for(int i = 0; i < WORKER_NUM; i++) {
            totalLine += parsers[i].readLineCnt;
            totalReadBytes += parsers[i].readByteCnt;
        }


        long time2 = System.currentTimeMillis();
	  //  readingThread.sleep(3000);
        System.out.println("TOTALLINE:" + totalLine);
        System.out.println("TOTALByte:" + totalReadBytes);
        System.out.println("elapsed time:" + (time2 - time1));
    }
}
