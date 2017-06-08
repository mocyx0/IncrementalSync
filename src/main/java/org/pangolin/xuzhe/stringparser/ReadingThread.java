package org.pangolin.xuzhe.stringparser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.pangolin.xuzhe.stringparser.Constants.WORKER_NUM;

/**
 * Created by ubuntu on 17-6-3.
 */
public class ReadingThread extends Thread {
    private static Logger logger = LoggerFactory.getLogger(ReadingThread.class);
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
            long begin = System.currentTimeMillis();
            for (String fileName : fileNameArray) {
                int fileNo = Integer.parseInt(fileName.substring(fileName.lastIndexOf("/")+1, fileName.length()-4));
                fis = new FileInputStream(new File(fileName));
                FileChannel channel = fis.getChannel();
                while(true) {
                    ByteBuffer buffer = pool.get();
                    int pos = (int)channel.position();
                    int n = channel.read(buffer);
                    if(n == -1) {
                        pool.put(buffer);
                        break;
                    }
                    buffer.limit(n);
                    int limit = buffer.position();
                    buffer.position(buffer.limit() - 1);
                    while(buffer.get() != (byte) '\n'){
                        buffer.position(buffer.position() - 2);
                    }
                    channel.position(channel.position() - (limit - buffer.position()));
                    buffer.flip();
                    int w = workerIndex%WORKER_NUM;
//                    pool.put(buffer);
                    workers[w].appendBuffer(buffer, pos, fileNo);
                    ++workerIndex;
                }
                channel.close();
                fis.close();
            }

            long end = System.currentTimeMillis();
            logger.info("Reading Done! elapsed time: {} ms", (end-begin));
            for(Worker worker : workers) {
                worker.appendBuffer(Worker.EMPTY_BUFFER, 0, 0);
            }
            for(Worker worker : workers) {
                worker.join();
            }
            for(Worker worker : workers) {
                logger.debug("Worker:{}", worker.getName());
                logger.debug("{}", worker.getIndexes());
            }
            end = System.currentTimeMillis();
            logger.info("Worker Done! elapsed time: {} ms", (end-begin));


        } catch (IOException e) {

        } catch (InterruptedException e) {

        }
    }

    public static void main(String[] args) throws InterruptedException {
        String[] fileNameArray = {"data/1.txt"};
        Long time1 = System.currentTimeMillis();
        ReadingThread readingThread = new ReadingThread(fileNameArray);
        readingThread.start();
        readingThread.join();
	    Long time2 = System.currentTimeMillis();
	  //  readingThread.sleep(3000);
	    logger.info("从pagecache读时，需要花费的时间：{} ms", time2-time1);
    }
}
