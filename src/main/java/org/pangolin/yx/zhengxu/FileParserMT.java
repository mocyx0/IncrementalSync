package org.pangolin.yx.zhengxu;

import org.pangolin.yx.Config;
import org.pangolin.yx.Util;
import org.slf4j.Logger;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/6/18.
 * 多线程日志解析
 */
public class FileParserMT implements FileParser {
    private static int FILE_BLOCK_COUNT = 128;
    private static int FILE_BLOCK_SIZE = 1024 * 1024;//不能大于32k
    //缓存多条数据后交给rebuilder处理
    private static int LOG_BUFFER_SIZE = 10240 / 2;
    private static int RESULT_QUEUE_SIZE = 12;
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

    //BlockingQueue<FileBlock> fileBlocks = new ArrayBlockingQueue<>(FILE_BLOCK_COUNT);

    ArrayList<BlockingQueue<FileBlock>> fileBlockQueues;

    //下一个需要读取的seq
    private int nextReadSeq = 0;

    //TreeMap<Integer, ArrayList<LogRecord>> logBlocks = new TreeMap<>();
    ArrayList<BlockingQueue<ArrayList<LogRecord>>> logBlocks = new ArrayList<>();


    ConcurrentLinkedQueue<byte[]> bufferPool = new ConcurrentLinkedQueue<>();


    private byte[] allocateReadBuff() {
        byte[] buff = bufferPool.poll();
        if (buff == null) {
            buff = new byte[FILE_BLOCK_SIZE];
        }
        return buff;
        //return new byte[FILE_BLOCK_SIZE];
    }

    private void freeReadBuff(byte[] buff) {
        bufferPool.offer(buff);
    }

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
                        fileBlock.buffer = allocateReadBuff();

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
                        fileBlockQueues.get(seq % fileBlockQueues.size()).put(fileBlock);
                        //fileBlocks.put(fileBlock);
                    }
                    raf.close();
                }

                //fileBlocks.put(new FileBlock());
                for (BlockingQueue<FileBlock> queue : fileBlockQueues) {
                    queue.put(new FileBlock());
                }
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
        BlockingQueue<FileBlock> queue;
        BlockingQueue<ArrayList<LogRecord>> resultQueue;

        ParseThread(CountDownLatch latch, BlockingQueue<FileBlock> queue, BlockingQueue<ArrayList<LogRecord>> resultQueue) {
            this.latch = latch;
            this.queue = queue;
            this.resultQueue = resultQueue;
        }

        LogRecord nextLineTest(byte[] data) {

            while (data[parsePos] != '\n') {
                parsePos++;
            }
            parsePos++;
            return null;
        }

        LogRecord nextLine(byte[] data) {
            TableInfo tableInfo = GlobalData.tableInfo;
            LogRecord logRecord = new LogRecord();
            //logRecord.lineData = data;
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
/*
            if (op == 'I') {
                insertCount.incrementAndGet();
            } else if (op == 'U') {
                updateCount.incrementAndGet();
            } else if (op == 'D') {
                deleteCount.incrementAndGet();
            }
            */
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
                    FileBlock fileBlock = queue.take();
                    if (fileBlock.buffer == null) {
                        //put back the flag
                        queue.put(fileBlock);
                        break;
                    } else {
                        ArrayList<LogRecord> logRecords = new ArrayList<>();
                        parsePos = 0;
                        /*
                        for (int i = 0; i < 1024 * 10; i++) {
                            logRecords.add(null);
                        }*/
                        int seq = fileBlock.seq;
                        while (parsePos < fileBlock.length) {
                            //byte[] newData = new byte[fileBlock.buffer.length];
                            //System.arraycopy(fileBlock.buffer, 0, newData, 0, newData.length);
                            LogRecord logRecord = nextLine(fileBlock.buffer);
                            logRecords.add(logRecord);
//                            lineCount.incrementAndGet();
                            selfLineCount++;
                        }
                        freeReadBuff(fileBlock.buffer);
                        //System.out.println(lineCount);
                        resultQueue.put(logRecords);
                        /*
                        synchronized (FileParserMT.class) {
                            logBlocks.put(seq, logRecords);
                            int firstKey = logBlocks.firstKey();
                            while (firstKey == nextReadSeq) {
                                //这是下一个需要处理的块
                                for (LogRecord logRecord : logBlocks.get(firstKey)) {
                                    // handleLog(logRecord);
                                }
                                nextReadSeq++;
                                logBlocks.remove(firstKey);
                                if (logBlocks.size() != 0) {
                                    firstKey = logBlocks.firstKey();
                                } else {
                                    break;
                                }
                            }
                        }
                        */
                    }
                }
                resultQueue.put(new ArrayList<LogRecord>());
                logger.info(String.format("ParseThread  line:%d ", selfLineCount));
                latch.countDown();
            } catch (Exception e) {
                logger.info("{}", e);
                System.exit(0);
            }
        }
    }

    private static volatile long logSeq = 0;

    private void handleLog(LogRecord logRecord) throws Exception {
        logRecord.seq = logSeq++;
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
    /*
    private void flushLogInMap() throws Exception {
        synchronized (FileParserMT.class) {
            for (Integer i : logBlocks.keySet()) {
                for (LogRecord logRecord : logBlocks.get(i)) {
                    handleLog(logRecord);
                }
            }
        }
    }
    */

    //
    private void dispatch() throws Exception {
        while (true) {
            boolean done = false;
            for (int i = 0; i < logBlocks.size(); i++) {
                ArrayList<LogRecord> logRecords = logBlocks.get(i).take();
                if (logRecords.size() == 0) {
                    done = true;
                }
                for (LogRecord logRecord : logRecords) {
                    handleLog(logRecord);
                }
            }
            if (done) {
                break;
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
        fileBlockQueues = new ArrayList<>(parserCount);
        CountDownLatch latch = new CountDownLatch(parserCount);
        for (int i = 0; i < Config.PARSER_THREAD; i++) {
            BlockingQueue<FileBlock> queue = new ArrayBlockingQueue<FileBlock>(FILE_BLOCK_COUNT);
            fileBlockQueues.add(queue);
            BlockingQueue<ArrayList<LogRecord>> logBlockQueue = new LinkedBlockingQueue<>(RESULT_QUEUE_SIZE);
            logBlocks.add(logBlockQueue);
            Thread th = new Thread(new ParseThread(latch, queue, logBlockQueue));
            th.start();
        }
        //
        //latch.await();
        //flush log in buffer
        //flushLogInMap();
        dispatch();
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
