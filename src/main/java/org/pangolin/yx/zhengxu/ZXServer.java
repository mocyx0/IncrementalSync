package org.pangolin.yx.zhengxu;

import com.alibaba.middleware.race.sync.Client;
import org.pangolin.yx.Config;
import org.pangolin.yx.ResultWriter;
import org.pangolin.yx.Util;
import org.pangolin.yx.WorkerServer;
import org.pangolin.yx.nixu.LogParser;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangxiao on 2017/6/16.
 */

class BlockData {
    LinkedBlockingQueue<ArrayList<LogRecord>> logQueue;
    ArrayList<LogRecord> buffQueue;
}

public class ZXServer implements WorkerServer {
    private static int BUFFER_SIZE = 1024;
    private CountDownLatch latch;
    private Logger logger;

    private ArrayList<BlockData> blockDatas = new ArrayList<>();

    private int queueCount;

    public ZXServer() {
        logger = Config.serverLogger;
    }

    @Override
    public void doTest() throws Exception {

    }

    public void startRebuilder() {
        int thCount = Config.REBUILDER_THREAD;
        latch = new CountDownLatch(thCount);
        queueCount = thCount;
        for (int i = 0; i < thCount; i++) {
            BlockData blockData = new BlockData();
            blockDatas.add(blockData);
            blockData.logQueue = new LinkedBlockingQueue<ArrayList<LogRecord>>(2048);
            blockData.buffQueue = new ArrayList<>();
            Thread th = new Thread(new Rebuilder(blockData.logQueue, latch, LineParser.tableInfo));
            th.start();
        }
    }

    private class ParserThread implements Runnable {

        private void startParser() throws Exception {
            long t1 = System.currentTimeMillis();
            ArrayList<String> paths = Util.logFiles(Config.DATA_HOME);
            LineParser.init(paths);
            long line = 0;
            LogRecord logRecord = LineParser.nextLine();
            while (logRecord != null) {
                line++;
                long id = logRecord.id;
                if (logRecord.opType == 'D') {
                    id = logRecord.preId;
                }
                int block = (int) (id % queueCount);
                //logQueues.get(block).put(logRecord);
                BlockData blockData = blockDatas.get(block);
                if (blockData.buffQueue.size() == BUFFER_SIZE) {
                    blockData.logQueue.put(blockData.buffQueue);
                    blockData.buffQueue = new ArrayList<>(BUFFER_SIZE);
                }
                blockData.buffQueue.add(logRecord);
                logRecord = LineParser.nextLine();
            }
            //end
            for (BlockData blockData : blockDatas) {
                blockData.logQueue.put(blockData.buffQueue);
                blockData.logQueue.put(new ArrayList<LogRecord>());
                blockData.buffQueue = null;
            }
            //
            long t2 = System.currentTimeMillis();
            logger.info(String.format("parse end, cost:%d", t2 - t1));
        }

        @Override
        public void run() {
            try {
                startParser();
            } catch (Exception e) {
                logger.info("{}", e);
                System.exit(0);
            }

        }
    }


    private void startParser1() throws Exception {
        long t1 = System.currentTimeMillis();
        ArrayList<String> paths = Util.logFiles(Config.DATA_HOME);
        LineParserDirect.init(paths);
        long line = 0;
        LogRecord logRecord = LineParserDirect.nextLine();
        while (logRecord != null) {
            line++;
            long id = logRecord.id;
            if (logRecord.opType == 'D') {
                id = logRecord.preId;
            }
            int block = (int) (id % queueCount);
            //logQueues.get(block).put(logRecord);
            logRecord = LineParserDirect.nextLine();
        }
        long t2 = System.currentTimeMillis();
        logger.info(String.format("parse end, cost:%d", t2 - t1));
    }

    @Override
    public void doData() throws Exception {
        //首先读取列信息
        LineParser.readTableInfo();
        //开启rebuilder线程
        startRebuilder();
        //解析线程
        Thread th = new Thread(new ParserThread());
        th.start();
        latch.await();
        logger.info("Rebuild done");

        //
        logger.info("Rebuild done");
        //直接读取
        //startParser1();

        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.put((byte) 0);
        buffer.flip();
        ResultWriter.writeBuffer(buffer);
    }
}
