package org.pangolin.xuzhe;

import static org.pangolin.xuzhe.Constants.WORKER_NUM;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ReadingThread extends Thread {
    String[] fileNameArray;
    Worker[] workers;
    public ReadingThread(String[] fileNameArray) {
        super("ReadingThread");
        this.fileNameArray = fileNameArray;
        workers = new Worker[WORKER_NUM];
        for(int i = 0; i < WORKER_NUM; i++) {
            workers[i] = new Worker();
        }
        for(int i = 0; i < WORKER_NUM; i++) {
            workers[i].start();
        }
    }

    @Override
    public void run() {
        ByteBufferPool pool = ByteBufferPool.getInstance();
        int workerIndex = 0;
        FileInputStream fis = null;
        try {
        	long fileNo = 0;
            for (String fileName : fileNameArray) {
            	fis = new FileInputStream(new File(fileName));
                FileChannel channel = fis.getChannel();
                while(true) {
                    ByteBuffer buffer = pool.get();
                    long pos = channel.position();
                    pos = (fileNo << 60) | pos;
                    int n = channel.read(buffer);
                    if(n == -1) {
                        pool.put(buffer);
                        break;
                    }
                    int limit = buffer.position();
                    buffer.position(buffer.limit() - 1);
                    while(buffer.get() != (byte) '\n'){
                	    buffer.position(buffer.position() - 2);
                    }
                    channel.position(channel.position() - (limit - buffer.position()));
                    buffer.flip();
                    int w = workerIndex%WORKER_NUM;
                    workers[w].appendBuffer(buffer, pos);
                }
                channel.close();
                fis.close();
                fileNo += 1;
            }

            System.out.println("Reading Done!");
            for(Worker worker : workers) {
                worker.appendBuffer(Worker.EMPTY_BUFFER, 0);
            }
            for(Worker worker : workers) {
                worker.join();
            }


        } catch (IOException e) {

        } catch (InterruptedException e) {

        }
    }


    public static void main(String[] args) throws InterruptedException {
        String[] fileNameArray = {"data/data_example.txt"};
        Long time1 = System.currentTimeMillis();
        ReadingThread readingThread = new ReadingThread(fileNameArray);
        readingThread.start();
        readingThread.join();
	    Long time2 = System.currentTimeMillis();
	  //  readingThread.sleep(3000);
	    System.out.println(time2 - time1);
    }
}
