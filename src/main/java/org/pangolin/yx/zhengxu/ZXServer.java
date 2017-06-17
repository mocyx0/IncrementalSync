package org.pangolin.yx.zhengxu;

import com.alibaba.middleware.race.sync.Client;
import org.pangolin.yx.Config;
import org.pangolin.yx.ResultWriter;
import org.pangolin.yx.Util;
import org.pangolin.yx.WorkerServer;
import org.pangolin.yx.nixu.LogParser;
import org.slf4j.Logger;

import java.nio.BufferOverflowException;
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
    private ArrayList<Rebuilder> rebuilders = new ArrayList<>();
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
            Rebuilder rebuilder = new Rebuilder(blockData.logQueue, latch, LineParser.tableInfo);
            Thread th = new Thread(rebuilder);
            rebuilders.add(rebuilder);
            th.start();
        }
    }

    private void pushLog(int block, LogRecord log) throws Exception {
        BlockData blockData = blockDatas.get(block);
        if (blockData.buffQueue.size() == BUFFER_SIZE) {
            blockData.logQueue.put(blockData.buffQueue);
            blockData.buffQueue = new ArrayList<>(BUFFER_SIZE);
        }
        blockData.buffQueue.add(log);
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
                if (logRecord.opType == 'U' && logRecord.preId != logRecord.id) {
                    //发送一个X消息表示消息已经被update
                    LogRecord xlog = new LogRecord();
                    xlog.opType = 'X';
                    xlog.id = logRecord.preId;
                    int block1 = (int) (xlog.id % queueCount);
                    pushLog(block1, xlog);
                }
                int block = (int) (id % queueCount);
                pushLog(block, logRecord);
                //logQueues.get(block).put(logRecord);

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

    private class Collector implements Runnable {
        private int BUFFER_SIZE = 1024 * 64;

        long start;//open
        long end;//close
        long pos;
        int index;
        int seq = 0;
        DataCollector dataCollector;
        CountDownLatch latch;
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        ByteBuffer sendBuffer = ByteBuffer.allocate(128);

        public Collector(DataCollector dataCollector, long s, long e, CountDownLatch latch, int inxex) {
            this.dataCollector = dataCollector;
            start = s;
            end = e;
            this.latch = latch;
            this.index = inxex;
        }

        private void flushData() throws Exception {
            buffer.flip();
            sendBuffer.putInt(index);
            sendBuffer.putInt(seq);
            sendBuffer.putInt(buffer.limit());
            sendBuffer.flip();

            synchronized (ResultWriter.class) {
                ResultWriter.writeBuffer(sendBuffer);
                ResultWriter.writeBuffer(buffer);
            }

            buffer.clear();
            sendBuffer.clear();
        }

        @Override
        public void run() {
            try {
                pos = start;
                while (pos < end) {
                    int position = buffer.position();
                    try {
                        dataCollector.writeBuffer(pos, buffer);
                        pos++;
                    } catch (BufferOverflowException e) {
                        buffer.position(position);
                        flushData();
                        seq++;
                    }
                }
                if (buffer.position() != 0) {
                    flushData();
                }
                latch.countDown();
            } catch (Exception e) {
                logger.info("{}", e);
                System.exit(0);
            }
        }
    }

    private void startCollector(ArrayList<DataStorage> dataStorages) throws Exception {
        long start = Config.queryData.start + 1;
        long end = Config.queryData.end;
        int thCount = Config.COLLECTOR_THREAD;
        long len = (end - start) / thCount;
        CountDownLatch latch = new CountDownLatch(thCount);
        for (int i = 0; i < thCount; i++) {
            long s = start + i * len;
            long e = start + i * len + len;
            if (i == thCount - 1) {
                e = end;
            }
            DataCollector dataCollector = new DataCollectorHashMap(dataStorages);
            Thread th = new Thread(new Collector(dataCollector, s, e, latch, i + 1));
            th.start();
        }
        latch.await();

        //write end
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.put((byte) 0);
        buffer.flip();
        ResultWriter.writeBuffer(buffer);
    }

    @Override
    public void doData() throws Exception {
        //Thread.sleep(3000);
        //首先读取列信息
        LineParser.readTableInfo();
        //开启rebuilder线程
        startRebuilder();
        //解析线程
        Thread th = new Thread(new ParserThread());
        th.start();
        latch.await();
        logger.info("Rebuild done");
        ArrayList<DataStorage> dataStorages = new ArrayList<>();
        for (Rebuilder rebuilder : rebuilders) {
            dataStorages.add(rebuilder.getDataStorage());
        }
        startCollector(dataStorages);
        //
        logger.info("collect done");
        //直接读取
        //startParser1();

        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(0);
        buffer.flip();
        ResultWriter.writeBuffer(buffer);
    }
}
