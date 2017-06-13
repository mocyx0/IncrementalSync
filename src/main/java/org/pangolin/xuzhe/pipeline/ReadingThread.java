package org.pangolin.xuzhe.pipeline;

import org.pangolin.yx.Config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.pangolin.xuzhe.pipeline.Constants.BUFFER_SIZE;
import static org.pangolin.xuzhe.pipeline.Constants.WORKER_NUM;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ReadingThread extends Thread {
    String[] fileNameArray;
    Parser[] parsers;
    public ReadingThread(String[] fileNameArray) {
        super("ReadingThread");
        this.fileNameArray = fileNameArray;
        parsers = new Parser[WORKER_NUM];
        for(int i = 0; i < WORKER_NUM; i++) {
            parsers[i] = new Parser();
        }
        for(int i = 0; i < WORKER_NUM; i++) {
            parsers[i].start();
        }
    }

    @Override
    public void run() {
        ByteBufferPool pool = ByteBufferPool.getInstance();
        int workerIndex = 0;
        FileInputStream fis;
        try {
            for (String fileName : fileNameArray) {
            	fis = new FileInputStream(new File(fileName));
                FileChannel channel = fis.getChannel();
                long fileSize = channel.size();
                long pos = fileSize - BUFFER_SIZE;
                int lastReadCnt;
                int remain = (int)fileSize;
                while(true) {

                    ByteBuffer buffer = pool.get();
                    if(remain < BUFFER_SIZE) {
                        buffer.put((byte)'\n');
                        buffer.limit(remain+1);
                    }
                    lastReadCnt = channel.read(buffer, pos);
                    remain = remain-lastReadCnt;
                    pos =  Math.max(pos-lastReadCnt, 0);
                    buffer.flip();
                    int w = workerIndex%WORKER_NUM;
                    parsers[w].appendBuffer(buffer);
                    if(remain == 0) {
                        break;
                    }
                }
                channel.close();
                fis.close();
            }

            System.out.println("Reading Done!");
            for(Parser parser : parsers) {
                parser.appendBuffer(Parser.EMPTY_BUFFER);
            }
            for(Parser parser : parsers) {
                parser.join();
            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws InterruptedException {
        Config.init();
        String[] fileNameArray = { Config.DATA_HOME + "/2.txt"};
        Long time1 = System.currentTimeMillis();
        ReadingThread readingThread = new ReadingThread(fileNameArray);
        readingThread.start();
        readingThread.join();
	    Long time2 = System.currentTimeMillis();
	  //  readingThread.sleep(3000);
	    System.out.println(time2 - time1);
    }
}
