package org.pangolin.xuzhe;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.pangolin.xuzhe.Constants.WORKER_NUM;

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
        try {
            for (String fileName : fileNameArray) {
                FileChannel channel = new FileInputStream(new File(fileName)).getChannel();
                while(true) {
                    ByteBuffer buffer = pool.get();
                    int n = channel.read(buffer);
                    if(n == -1) {
                        pool.put(buffer);
                        break;
                    }
                    buffer.flip();
                    workers[workerIndex%WORKER_NUM].appendBuffer(buffer);
                    workerIndex++;
                }
            }
            for(Worker worker : workers) {
                worker.appendBuffer(Worker.EMPTY_BUFFER);
            }
            for(Worker worker : workers) {
                worker.join();
            }
        } catch (IOException e) {

        } catch (InterruptedException e) {

        }
    }


    public static void main(String[] args) throws InterruptedException {
        String[] fileNameArray = {"data/1.txt"};
        ReadingThread readingThread = new ReadingThread(fileNameArray);
        readingThread.start();
        readingThread.join();
    }
}
