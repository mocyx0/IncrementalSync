package org.pangolin.yx.zhengxu;

import com.alibaba.middleware.race.sync.Client;
import org.pangolin.yx.Config;
import org.pangolin.yx.ResultWriter;
import org.pangolin.yx.Util;
import org.pangolin.yx.WorkerServer;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangxiao on 2017/6/16.
 */
public class ZXServer implements WorkerServer {

    private Logger logger;
    ArrayList<LinkedBlockingQueue<LogRecord>> logQueues = new ArrayList<>();
    private int queueCount;

    public ZXServer() {
        logger = Config.serverLogger;
    }

    @Override
    public void doTest() throws Exception {

    }

    public void startRebuilder() {
        int thCount = Config.CPU_COUNT - 1;
        queueCount = thCount;
        for (int i = 0; i < thCount; i++) {
            logQueues.add(new LinkedBlockingQueue<LogRecord>(1000));
        }

    }

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
            logRecord = LineParser.nextLine();
        }
        long t2 = System.currentTimeMillis();
        logger.info(String.format("parse end, cost:%d", t2 - t1));
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
        startRebuilder();

        startParser();

        startParser1();

        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.put((byte) 0);
        buffer.flip();
        ResultWriter.writeBuffer(buffer);
    }
}
