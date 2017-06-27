package org.pangolin.yx.zhengxu;

import com.alibaba.middleware.race.sync.Constants;
import org.pangolin.yx.Config;
import org.pangolin.yx.Util;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangxiao on 2017/6/18.
 * 单线程读取解析
 */

public class FileParserSimple implements FileParser {
    Logger logger;

    private class BlockData {
        LinkedBlockingQueue<ArrayList<LogRecord>> logQueue;
        ArrayList<LogRecord> buffQueue;
    }

    private ArrayList<BlockData> blockDatas = new ArrayList<>();
    private static int BUFFER_SIZE = 1024;
    LogQueues queues;
    int queueCount;
    ArrayList<ArrayList<LogRecord>> logBuffers = new ArrayList<>();

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

    @Override
    public void run(LogQueues queues) {
        logger = Config.serverLogger;
        this.queues = queues;
        queueCount = this.queues.queues.size();
        for (int i = 0; i < queues.queues.size(); i++) {
            BlockData blockData = new BlockData();
            //TODO
           // blockData.logQueue = queues.queues.get(i);
            blockData.buffQueue = new ArrayList<>(BUFFER_SIZE);
            blockDatas.add(blockData);
        }
        Thread th = new Thread(new ParserThread());
        th.start();
    }
}
