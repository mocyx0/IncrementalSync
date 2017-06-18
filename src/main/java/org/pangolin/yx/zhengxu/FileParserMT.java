package org.pangolin.yx.zhengxu;

import org.pangolin.yx.Config;
import org.pangolin.yx.Util;
import org.slf4j.Logger;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/6/18.
 * 多线程日志解析
 */
public class FileParserMT implements FileParser {
    private static int FILE_BLOCK_COUNT = 1024;
    private static int FILE_BLOCK_SIZE = 1024 * 32;//不能大于32k
    private static int LOG_BUFFER_SIZE = 1024;
    private LogQueues queues;
    private ArrayList<BlockData> blockDatas = new ArrayList<>();
    int queueCount;

    private static class FileBlock {
        byte[] buffer;
        int seq;
        int length;
    }

    private Logger logger;

    private class BlockData {
        LinkedBlockingQueue<ArrayList<LogRecord>> logQueue;
        ArrayList<LogRecord> buffQueue;
    }

    BlockingQueue<FileBlock> fileBlocks = new LinkedBlockingQueue<>(FILE_BLOCK_COUNT);

    //下一个需要读取的seq
    private int nextReadSeq = 0;

    TreeMap<Integer, ArrayList<LogRecord>> logBlocks = new TreeMap<>();


    private class ReadThread implements Runnable {
        @Override
        public void run() {
            try {
                int seq = 0;
                ArrayList<String> files = Util.logFiles(Config.DATA_HOME);
                for (int i = 0; i < files.size(); i++) {
                    String path = files.get(i);
                    RandomAccessFile raf = new RandomAccessFile(path, "r");
                    long fileLen = raf.length();
                    long pos = 0;
                    while (pos < fileLen) {
                        FileBlock fileBlock = new FileBlock();
                        fileBlock.buffer = new byte[FileParserMT.FILE_BLOCK_SIZE];
                        fileBlock.length = raf.read(fileBlock.buffer);
                        //find the last \n, so we have a full line
                        int last = fileBlock.length;
                        while (fileBlock.buffer[last - 1] != '\n') {
                            last--;
                        }
                        //
                        fileBlock.length = last;
                        fileBlock.seq = seq;
                        seq++;
                        pos += fileBlock.length;
                        raf.seek(pos);
                        fileBlocks.put(fileBlock);
                    }
                    raf.close();
                }
                fileBlocks.put(new FileBlock());
                logger.info("ReadThread done");
            } catch (Exception e) {
                logger.info("{}", e);
                System.exit(0);
            }
        }
    }

    public static AtomicInteger updateCount = new AtomicInteger();
    public static AtomicInteger insertCount = new AtomicInteger();
    public static AtomicInteger deleteCount = new AtomicInteger();
    public static AtomicInteger pkUpdate = new AtomicInteger();
    public static AtomicInteger lineCount = new AtomicInteger();

    private void pushLog(int block, LogRecord log) throws Exception {
        BlockData blockData = blockDatas.get(block);
        if (blockData.buffQueue.size() == LOG_BUFFER_SIZE) {
            blockData.logQueue.put(blockData.buffQueue);
            blockData.buffQueue = new ArrayList<>(LOG_BUFFER_SIZE);
        }
        blockData.buffQueue.add(log);
    }


    private class ParseThread implements Runnable {

        int parsePos = 0;
        CountDownLatch latch;

        ParseThread(CountDownLatch latch) {
            this.latch = latch;
        }

        LogRecord nextLine(byte[] data) {
            TableInfo tableInfo = GlobalData.tableInfo;
            LogRecord logRecord = new LogRecord();
            logRecord.lineData = data;
            logRecord.columnData = new short[3 * (tableInfo.columnName.length - 1)];
            int colWriteIndex = 0;

            int pos = parsePos;
            pos = pos + 1 + ZXUtil.nextToken(data, pos, '|');
            pos = pos + 1 + ZXUtil.nextToken(data, pos, '|');//uid
            pos = pos + 1 + ZXUtil.nextToken(data, pos, '|');//time
            pos = pos + 1 + ZXUtil.nextToken(data, pos, '|');//scheme
            pos = pos + 1 + ZXUtil.nextToken(data, pos, '|');//table
            int opPos = pos;
            byte op = data[opPos];
            logRecord.opType = op;
            pos = pos + 1 + ZXUtil.nextToken(data, pos, '|');//op

            if (op == 'I') {
                insertCount.incrementAndGet();
            } else if (op == 'U') {
                updateCount.incrementAndGet();
            } else if (op == 'D') {
                deleteCount.incrementAndGet();
            }

            while (data[pos] != '\n') {
                int namePos = pos;
                int nameLen = ZXUtil.nextToken(data, pos, ':');//col name
                pos += 1 + nameLen;
                byte type = data[pos];
                byte isPk = data[pos + 2];
                pos += 4;//skip
                int oldPos = pos;
                int oldLen = ZXUtil.nextToken(data, pos, '|');
                pos = pos + 1 + oldLen;//old value
                int newPos = pos;
                int newLen = ZXUtil.nextToken(data, pos, '|');
                pos = pos + 1 + newLen;//new value
                if (isPk == '1') {
                    if (op == 'I') {
                        logRecord.id = ZXUtil.parseLong(data, newPos, newLen);
                    } else if (op == 'D') {
                        logRecord.preId = ZXUtil.parseLong(data, oldPos, oldLen);
                    } else if (op == 'U') {
                        logRecord.id = ZXUtil.parseLong(data, newPos, newLen);
                        logRecord.preId = ZXUtil.parseLong(data, oldPos, oldLen);
                        if (logRecord.id != logRecord.preId) {
                            pkUpdate.incrementAndGet();
                        }
                    }
                } else {
                    int byteIndex = tableInfo.getColumnIndex(data, namePos, nameLen);
                    logRecord.columnData[colWriteIndex++] = (short) byteIndex;
                    logRecord.columnData[colWriteIndex++] = (short) newPos;
                    logRecord.columnData[colWriteIndex++] = (short) newLen;
                }
            }
            pos++;//skip \n
            parsePos = pos;
            return logRecord;
        }

        @Override
        public void run() {
            try {
                int selfLineCount = 0;
                while (true) {
                    FileBlock fileBlock = fileBlocks.take();
                    if (fileBlock.buffer == null) {
                        //put back the flag
                        fileBlocks.put(fileBlock);
                        break;
                    } else {
                        ArrayList<LogRecord> logRecords = new ArrayList<>();
                        parsePos = 0;
                        int seq = fileBlock.seq;
                        while (parsePos < fileBlock.length) {

                            LogRecord logRecord = nextLine(fileBlock.buffer);
                            logRecords.add(logRecord);
                            lineCount.incrementAndGet();
                            selfLineCount++;
                        }
                        //System.out.println(lineCount);
                        synchronized (FileParserMT.class) {
                            logBlocks.put(seq, logRecords);
                            int firstKey = logBlocks.firstKey();
                            if (firstKey == nextReadSeq) {
                                //这是下一个需要处理的块
                                for (LogRecord logRecord : logBlocks.get(firstKey)) {
                                    handleLog(logRecord);
                                }
                                nextReadSeq++;
                                logBlocks.remove(firstKey);
                            }

                        }
                    }
                }
                logger.info(String.format("ParseThread  line:%d ", selfLineCount));
                latch.countDown();
            } catch (Exception e) {
                logger.info("{}", e);
                System.exit(0);
            }
        }
    }

    private void handleLog(LogRecord logRecord) throws Exception {
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
    }

    private void flushLogInMap() throws Exception {
        synchronized (FileParserMT.class) {
            for (Integer i : logBlocks.keySet()) {
                for (LogRecord logRecord : logBlocks.get(i)) {
                    handleLog(logRecord);
                }
            }
        }
    }


    @Override
    public void run(LogQueues queues) throws Exception {
        //init log buffer
        logger = Config.serverLogger;
        this.queues = queues;
        queueCount = this.queues.queues.size();
        for (int i = 0; i < queues.queues.size(); i++) {
            BlockData blockData = new BlockData();
            blockData.logQueue = queues.queues.get(i);
            blockData.buffQueue = new ArrayList<>(LOG_BUFFER_SIZE);
            blockDatas.add(blockData);
        }
        //start working thread
        Thread readThread = new Thread(new ReadThread());
        readThread.start();
        int parserCount = Config.PARSER_THREAD;
        CountDownLatch latch = new CountDownLatch(parserCount);
        for (int i = 0; i < Config.PARSER_THREAD; i++) {
            Thread th = new Thread(new ParseThread(latch));
            th.start();
        }
        latch.await();
        //flush log in buffer
        flushLogInMap();
        logger.info(String.format("line:%d insert:%d update:%d delete:%d pkupdate:%d ",
                lineCount.get(), insertCount.get(), updateCount.get(), deleteCount.get(), pkUpdate.get()));
        //send a empty data
        for (BlockData blockData : blockDatas) {
            blockData.logQueue.put(blockData.buffQueue);
            blockData.logQueue.put(new ArrayList<LogRecord>());
            blockData.buffQueue = null;
        }
        //
    }
}
