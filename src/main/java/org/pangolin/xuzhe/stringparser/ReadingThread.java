package org.pangolin.xuzhe.stringparser;

import org.pangolin.yx.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

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
                    // 将多读的不足半行的数据退回
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
//            for(Worker worker : workers) {
//                logger.debug("Worker:{}", worker.getName());
//                logger.debug("{}", worker.getIndexes());
//            }
//            LocalLogIndex[] localLogIndices = new LocalLogIndex[WORKER_NUM];
//            for(int i = 0; i < WORKER_NUM; ++i) {
//                localLogIndices[i] = workers[i].getIndexes();
//            }
            LocalLogIndex allIndexex =workers[0].getIndexes();
//            System.out.println(allIndexex.indexes.size());
            end = System.currentTimeMillis();
            logger.info("Worker Done! elapsed time: {} ms", (end-begin));
             searchResult(allIndexex);
            logger.info("Worker Done! elapsed time: {} ms", (end - begin));
    //      searchTest(allIndexex);
 //            searchTest(allIndexex);
        } catch (IOException e) {

        } catch (InterruptedException e) {


        } catch (Exception e) {
            logger.info("{}", e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Config.init();
        String[] fileNameArray = {Config.DATA_HOME + "/1.txt"};
        Long time1 = System.currentTimeMillis();
        ReadingThread readingThread = new ReadingThread(fileNameArray);
        readingThread.start();
        readingThread.join();
	    Long time2 = System.currentTimeMillis();
	  //  readingThread.sleep(3000);
	    logger.info("从pagecache读时，需要花费的时间：{} ms", time2-time1);
    }

    public static void searchTest(LocalLogIndex indexes) {
        Scanner scanner = new Scanner(System.in);
        while(scanner.hasNext()) {
            String line = scanner.nextLine().trim();
            if(line.startsWith("quit")) break;
            try {
                Long pk = Long.valueOf(line);
                List<LocalLogIndex.IndexEntry> logs = indexes.indexes.get(pk);
                Collections.sort(logs);
                for(LocalLogIndex.IndexEntry index : logs) {
                    System.out.println(String.format("%d:%10d %s", index.fileNo, index.position,  getLineByPosition(index.fileNo, index.position)));
                }
            } catch (Exception e) {

            }
        }
    }
    public static void searchResult(LocalLogIndex indexes) {
        Scanner scanner = new Scanner(System.in);
        while(scanner.hasNext()) {
            String line = scanner.nextLine().trim();
            if(line.startsWith("quit")) break;
            try {
                Long pk = Long.valueOf(line);
                Record result = Redo.redo(indexes, pk);

                System.out.print(result.getPk() + " ");
                for(Map.Entry<String, Object> entry : result.getValues().entrySet()){
                    //           System.out.print(entry.getKey() + " " + String.valueOf(entry.getValue()+ " "));
                    if(!entry.getKey().equals("id")) {
                        System.out.print(String.valueOf(entry.getValue() + " "));
                    }
                }
            } catch (Exception e) {

            }
        }
    }
    private static Map<Integer, FileChannel> fileMap = new HashMap<>();
    private static ByteBuffer lineBuffer = ByteBuffer.allocate(1<<10);
    public static String getLineByPosition(int fileNo, int position) {
        FileChannel fileChannel = fileMap.get(fileNo);
        try {
            if(fileChannel == null) {
                fileChannel = new RandomAccessFile(Config.DATA_HOME + "/" + fileNo + ".txt", "r").getChannel();
                fileMap.put(fileNo, fileChannel);
            }
            lineBuffer.clear();
            fileChannel.read(lineBuffer, position);
            lineBuffer.flip();
            while(lineBuffer.get() != '\n') {
                ; // no op
            }
            int lineLength = lineBuffer.position()-1;
            String str = new String(lineBuffer.array(), 0, lineLength, "utf-8");
            return str;
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }
}
