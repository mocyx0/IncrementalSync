package org.pangolin.yx.zhengxu;

import org.pangolin.yx.*;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * Created by yangxiao on 2017/6/16.
 */

class LogQueues {
    ArrayList<BlockingQueue<LogBlock>> queues = new ArrayList<>();
}

class MBuffer {
    byte[] data;
    int len = 0;
}

public class ZXServer implements WorkerServer {
    private CountDownLatch latch;
    private LogQueues logQueues = new LogQueues();
    private ArrayList<Rebuilder> rebuilders = new ArrayList<>();
    private int queueCount;

    public ZXServer() {
    }

    @Override
    public void doTest() throws Exception {

    }

    public void startRebuilder() {
        int thCount = Config.REBUILDER_THREAD;
        latch = new CountDownLatch(thCount);
        queueCount = thCount;
        for (int i = 0; i < thCount; i++) {
            BlockingQueue<LogBlock> logQueue = new ArrayBlockingQueue<LogBlock>(Config.REBUILDER_IN_QUEUE);
            logQueues.queues.add(logQueue);
            Rebuilder rebuilder = new Rebuilder(logQueue, latch, LineParser.tableInfo, i, thCount);
            Thread th = new Thread(rebuilder);
            rebuilders.add(rebuilder);
            th.start();
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
        MLog.info(String.format("parse end, cost:%d", t2 - t1));
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

//        byte[] dataBuff = new byte[BUFFER_SIZE];
        //       int dataPos = 0;

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

        private byte[] pkBuffer = new byte[64];

        @Override
        public void run() {
            try {

                pos = start;
                while (pos < end) {
                    int position = buffer.position();
                    try {
                        dataCollector.writeBuffer(pos, buffer, pkBuffer);
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
                MLog.info(e);
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
            //DataCollector dataCollector = new DataCollectorHashMap(dataStorages);
            DataCollector dataCollector = new DataCollectorTwoLevel(dataStorages);
            Thread th = new Thread(new Collector(dataCollector, s, e, latch, i + 1));
            th.start();
        }
        latch.await();

        //write end
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(0);
        buffer.flip();
        ResultWriter.writeBuffer(buffer);
        ResultWriter.close();

    }

    //FileParser fileParser = new FileParserSimple();
    FileParser fileParser = new FileParserMT();

    @Override
    public void doData() throws Exception {
        ZXUtil.init();
        //Thread.sleep(3000);
        //首先读取列信息
        long t1 = System.currentTimeMillis();
        // System.out.println(t1);
        LineParser.readTableInfo();
        DataStorageTwoLevel.init(GlobalData.tableInfo);

        LogBlock.init();
        //System.out.println(System.currentTimeMillis());
        ReadBufferPoll.init();

        //开启rebuilder线程
        startRebuilder();

        //解析线程
        fileParser.run(logQueues);
        latch.await();

        long t2 = System.currentTimeMillis();
        MLog.info(String.format("Rebuild done cost %d", t2 - t1));
        ArrayList<DataStorage> dataStorages = new ArrayList<>();
        for (Rebuilder rebuilder : rebuilders) {
            dataStorages.add(rebuilder.getDataStorage());
        }
        startCollector(dataStorages);
        //
        long t3 = System.currentTimeMillis();
        MLog.info(String.format("collect done cost %d", t3 - t2));
        System.exit(0);
    }
}
