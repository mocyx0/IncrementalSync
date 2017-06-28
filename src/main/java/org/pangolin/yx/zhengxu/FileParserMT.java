package org.pangolin.yx.zhengxu;

import org.pangolin.yx.Config;
import org.pangolin.yx.MLog;
import org.pangolin.yx.ReadBufferPoll;
import org.pangolin.yx.Util;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/6/18.
 * 多线程日志解析
 */
class LogBlock {
    public static LogBlock EMPTY = new LogBlock();
    //ArrayList<LogRecord> logRecords = new ArrayList<>();
//    ArrayList<ArrayList<LogRecord>> logRecordsArr = new ArrayList<>();
    public static final int MAX_LENGTH = 100000;
    long[] ids = new long[MAX_LENGTH];
    int[] colDataInfo = new int[MAX_LENGTH];//高位3个字节位置: 低位1个字节:长度
    long[] preIds = new long[MAX_LENGTH];
    byte[] opTypes = new byte[MAX_LENGTH];
    byte[] redoer = new byte[MAX_LENGTH];
    //int[] columnData = new int[GlobalData.colCount * 3 * MAX_LENGTH];//三个一个单位  列索引, 位置, 长度
    long[] colData = new long[GlobalData.colCount * MAX_LENGTH];
    long[] seqs = new long[MAX_LENGTH];//log sequence
    int length = 0;

    //LogBlockRebuilder[] logBlockRebuilders = new LogBlockRebuilder[Config.REBUILDER_THREAD];

    //引用
    AtomicInteger ref = new AtomicInteger();

    private LogBlock() {
        /*
        for (int i = 0; i < logBlockRebuilders.length; i++) {
            logBlockRebuilders[i] = new LogBlockRebuilder();
        }
        */
    }

    public static void init() throws Exception {
        for (int i = 0; i < Config.LOG_BLOCK_QUEUE; i++) {
            queue.put(new LogBlock());
        }
    }

    private static BlockingDeque<LogBlock> queue = new LinkedBlockingDeque<>(Config.LOG_BLOCK_QUEUE);

    public static LogBlock allocate() throws Exception {
        LogBlock logBlock = queue.takeFirst();
        return logBlock;
    }

    public static void free(LogBlock logBlock) throws Exception {
        //logBlock.fileBlock = null;
        /*
        for (int i = 0; i < logBlock.logBlockRebuilders.length; i++) {
            logBlock.logBlockRebuilders[i].length = 0;
        }
        */
        logBlock.length = 0;
        queue.putFirst(logBlock);
    }

}

class LogBlockRebuilder {
    int length;
    int[] poss = new int[LogBlock.MAX_LENGTH];
}

class FileBlock {
    ByteBuffer buffer;//这个buff会被重复使用
    int seq;
    int length;
}

public class FileParserMT implements FileParser {
    private LogQueues queues;
    private ArrayList<BlockData> blockDatas = new ArrayList<>();
    int queueCount;


    private class BlockData {
        BlockingQueue<LogBlock> logQueue;
        //ArrayList<LogRecord> buffQueue;
    }

    //BlockingQueue<FileBlock> fileBlocks = new ArrayBlockingQueue<>(FILE_BLOCK_COUNT);

    ArrayList<BlockingQueue<FileBlock>> fileBlockQueues;

    //下一个需要读取的seq
    private int nextReadSeq = 0;

    //TreeMap<Integer, ArrayList<LogRecord>> logBlocks = new TreeMap<>();
    //这是每个parser输出的LogBlock
    ArrayList<BlockingQueue<LogBlock>> logBlocks = new ArrayList<>();


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
                        fileBlock.buffer = ReadBufferPoll.allocateReadBuff();
                        fileBlock.length = raf.getChannel().read(fileBlock.buffer);
                        //find the last \n, so we have a full line
                        int last = fileBlock.length;
                        DirectBuffer directBuffer = (DirectBuffer) fileBlock.buffer;
                        long add = directBuffer.address();
                        while (ZXUtil.unsafe.getByte(add + last - 1) != '\n') {//fileBlock.buffer[last - 1] != '\n'
                            last--;
                        }
                        //
                        fileBlock.length = last;
                        fileBlock.seq = seq;
                        pos += fileBlock.length;
                        raf.seek(pos);
                        fileBlockQueues.get(seq % fileBlockQueues.size()).put(fileBlock);
                        //fileBlocks.put(fileBlock);
                        seq++;
                    }
                    raf.close();
                }

                //fileBlocks.put(new FileBlock());
                for (BlockingQueue<FileBlock> queue : fileBlockQueues) {
                    queue.put(new FileBlock());
                }
                MLog.info("ReadThread done");
            } catch (Exception e) {
                MLog.info(e);
                System.exit(0);
            }
        }
    }

    public static AtomicInteger updateCount = new AtomicInteger();
    public static AtomicInteger insertCount = new AtomicInteger();
    public static AtomicInteger deleteCount = new AtomicInteger();
    public static AtomicInteger pkUpdate = new AtomicInteger();
    public static AtomicInteger lineCount = new AtomicInteger();

    /*
    private void pushLog(int block, LogRecord log) throws Exception {
        BlockData blockData = blockDatas.get(block);
        if (blockData.buffQueue.size() == LOG_BUFFER_SIZE) {
            blockData.logQueue.put(blockData.buffQueue);
            blockData.buffQueue = new ArrayList<>(LOG_BUFFER_SIZE);
        }
        blockData.buffQueue.add(log);
    }
    */

    private class ParseThread implements Runnable {

        int parsePos = 0;
        CountDownLatch latch;
        BlockingQueue<FileBlock> queue;
        BlockingQueue<LogBlock> resultQueue;
        int rebuilderCount;
        Unsafe unsafe;

        ParseThread(CountDownLatch latch, BlockingQueue<FileBlock> queue, BlockingQueue<LogBlock> resultQueue) {
            this.latch = latch;
            this.queue = queue;
            this.resultQueue = resultQueue;
            rebuilderCount = Config.REBUILDER_THREAD;
            unsafe = ZXUtil.unsafe;
        }

        long seqNumber = 0;

        int nextToken() {
            int old = parsePos;
            //while (data[parsePos] != delimit) {
            while (unsafe.getByte(buffAddr + parsePos) != '|') {
                parsePos++;
            }
            parsePos++;
            return parsePos - old - 1;
        }

        int nextColName() {
            //if (Config.OPTIMIZE) {
            int nameLen = 0;
            byte b1 = unsafe.getByte(buffAddr + parsePos);
            if (b1 == 'i') {
                nameLen = 2;
            } else if (b1 == 'f') {
                nameLen = 10;
            } else if (b1 == 'l') {
                nameLen = 9;
            } else if (b1 == 's') {
                if (unsafe.getByte(buffAddr + parsePos + 3) == ':') {
                    nameLen = 3;
                } else if (unsafe.getByte(buffAddr + parsePos + 5) == ':') {
                    nameLen = 5;
                } else if (unsafe.getByte(buffAddr + parsePos + 6) == ':') {
                    nameLen = 6;
                }
            }
            parsePos += nameLen + 1;
            return nameLen;
                /*
            } else {
                return nextToken(data, ':');
            }
            */
        }

        private long nextColValue() {
            int old = parsePos;
            colValue = 0;
            while (unsafe.getByte(buffAddr + parsePos) != '|') {
                colValue = colValue << 8 | ((long) unsafe.getByte(buffAddr + parsePos) & 0xff);
                parsePos++;
            }
            colValue = colValue << 8 | ((long) (parsePos - old) & 0xff);
            parsePos++;
            return colValue;
        }

        long parseLong() {
            if (unsafe.getByte(buffAddr + parsePos) == 'N') {
                parsePos += 5;
                return -1;
            } else {
                long v = 0;
                byte b = unsafe.getByte(buffAddr + parsePos);
                parsePos++;
                while (b != '|') {
                    v = v * 10 + (b - '0');
                    b = unsafe.getByte(buffAddr + parsePos);
                    parsePos++;
                }
                return v;
            }
        }

        int colParseIndex = 0;
        long colValue = 0;
        int colDataPos = 0;//分配coldata的位置

        void nextLineDirect() throws Exception {
            TableInfo tableInfo = GlobalData.tableInfo;
            // int pos = parsePos;
            long id = -1;
            long preid = -1;
            int logColPos = colDataPos;//保存起始位置
            byte op;
            if (Config.OPTIMIZE) {
                parsePos += 18;
                nextToken();
                parsePos += 34;
            } else {
                nextToken();
                nextToken();
                nextToken();
                nextToken();
                nextToken();
            }
            //op = data[parsePos];
            op = unsafe.getByte(buffAddr + parsePos);
            parsePos += 2;
            int logPos = logBlock.length;

            //boolean accept = true;
            colParseIndex = 0;
            while (unsafe.getByte(buffAddr + parsePos) != '\n') {
                if (colParseIndex == 0) {
                    parsePos += 7;
                    preid = parseLong();
                    id = parseLong();
                    /*
                    long activeId = id;
                    if (op == 'D') {
                        activeId = preid;
                    }
                    if (activeId >= Config.ALI_ID_MAX || activeId <= Config.ALI_ID_MIN) {
                        accept = false;
                        while (data[parsePos] != '\n') {
                            parsePos++;
                        }
                    }
                    */

                } else {
                    int namePos = parsePos;
                    int nameLen = nextColName();
                    parsePos += 4;
                    nextToken();//old value
                    long colValue = nextColValue();//new value
                    int colIndex = tableInfo.getColumnIndex(null, namePos, nameLen);
                    logBlock.colData[colDataPos++] = ((long) colIndex << 56) | colValue;
                }
                colParseIndex++;
            }
            parsePos++;//skip \n



/*
            boolean accept = true;
            if (Config.OPTIMIZE) {
                if (id >= Config.ALI_ID_MAX) {
                    accept = false;
                }
            }
            */
            //if (accept) {
            logBlock.ids[logPos] = id;
            logBlock.preIds[logPos] = preid;
            logBlock.opTypes[logPos] = op;
            //logBlock.seqs[logPos] = seqNumber++;
            //logBlock.seqs[logPos] = 0;
            logBlock.length++;
            byte colLen = (byte) (colDataPos - logColPos);
            logBlock.colDataInfo[logPos] = logColPos << 8 | colLen;
            if (op == 'D') {
                logBlock.redoer[logPos] = (byte) ((preid) % Config.REBUILDER_THREAD);
            } else {
                logBlock.redoer[logPos] = (byte) ((id) % Config.REBUILDER_THREAD);
            }
            // }
            if (preid != id && op == 'U') {
                int xpos = logBlock.length;
                logBlock.ids[xpos] = preid;
                logBlock.preIds[xpos] = -1;
                logBlock.opTypes[xpos] = 'X';
                logBlock.length++;
                logBlock.redoer[xpos] = (byte) ((preid) % Config.REBUILDER_THREAD);
            }
        }

        private void printColData(LogBlock logBlock, int logPos) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < GlobalData.colCount; i++) {
                long v = logBlock.colData[logPos * GlobalData.colCount + i];
                int len = (int) (v & 0xff);
                if (len == 0) {
                    break;
                } else {
                    for (int j = len - 1; j >= 0; j--) {
                        byte b = (byte) ((v >> (8 + j * 8)) & 0xff);
                        sb.append((char) b);
                    }
                    sb.append(" ");
                }
            }
            System.out.println(sb.toString());
        }

        DirectBuffer directBuffer;
        LogBlock logBlock;
        long buffAddr;

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
                        logBlock = LogBlock.allocate();
                        colDataPos = 0;
                        parsePos = 0;
                        seqNumber = (long) fileBlock.seq << 32;
                        directBuffer = (DirectBuffer) fileBlock.buffer;
                        buffAddr = directBuffer.address();

                        while (parsePos < fileBlock.length) {
                            nextLineDirect();
                            selfLineCount++;
                        }
                        ReadBufferPoll.freeReadBuff(fileBlock.buffer);
                        logBlock.ref.set(queueCount);
                        //TEST
                        resultQueue.put(logBlock);
                        //LogBlock.free(logBlock);
                    }
                }
                resultQueue.put(LogBlock.EMPTY);
                //MLog.info(String.format("ParseThread  line:%d  ", selfLineCount));
                latch.countDown();
            } catch (Exception e) {
                MLog.info(e);
                System.exit(0);
            }
        }

    }

    private static volatile long logSeq = 0;
/*
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
    */

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

    private static int bias1Count = 0;
    private static int bias2Count = 0;

    //
    private void dispatch() throws Exception {
        while (true) {
            boolean done = false;
            for (int i = 0; i < logBlocks.size(); i++) {
                LogBlock logBlock = logBlocks.get(i).take();

                /*
                int zeroCount = 0;
                //StringBuilder sb = new StringBuilder();
                for (LogBlockRebuilder logBlockRebuilder : logBlock.logBlockRebuilders) {
                    //  sb.append(logBlockRebuilder.length).append(" ");
                    if (logBlockRebuilder.length == 0) {
                        zeroCount++;
                    }
                }
                //System.out.println(logBlock.length);

                if (zeroCount > 0) {
                    bias1Count++;
                    //  sb.append(biasCount);
                    // sb.append("\n");
                    // System.out.println(sb.toString());
                }
                if (zeroCount > 1) {
                    bias2Count++;
                }
                */


                if (logBlock == LogBlock.EMPTY) {
                    done = true;
                } else {
                    for (BlockingQueue<LogBlock> queue : queues.queues) {
                        queue.put(logBlock);
                    }
                }
                /*
                for (LogRecord logRecord : logRecords) {
                    handleLog(logRecord);
                }
                */
            }
            if (done) {
                break;
            }
        }

    }

    @Override
    public void run(LogQueues queues) throws Exception {
        //init log buffer
        this.queues = queues;
        queueCount = this.queues.queues.size();
        for (int i = 0; i < queues.queues.size(); i++) {
            BlockData blockData = new BlockData();
            blockData.logQueue = queues.queues.get(i);
            //blockData.buffQueue = new ArrayList<>(LOG_BUFFER_SIZE);
            blockDatas.add(blockData);
        }
        //start working thread
        Thread readThread = new Thread(new ReadThread());
        readThread.start();
        int parserCount = Config.PARSER_THREAD;
        fileBlockQueues = new ArrayList<>(parserCount);
        CountDownLatch latch = new CountDownLatch(parserCount);
        for (int i = 0; i < Config.PARSER_THREAD; i++) {
            BlockingQueue<FileBlock> queue = new ArrayBlockingQueue<FileBlock>(Config.PARSER_IN_QUEUE);
            fileBlockQueues.add(queue);

            BlockingQueue<LogBlock> logBlockQueue = new ArrayBlockingQueue<LogBlock>(Config.PARSER_OUT_QUEUE);
            logBlocks.add(logBlockQueue);
            Thread th = new Thread(new ParseThread(latch, queue, logBlockQueue));
            th.start();
        }
        //
        //latch.await();
        //flush log in buffer
        //flushLogInMap();
        dispatch();
        //  logger.info(String.format("%d %d",DataStoragePlain.bigIdCount.get(),DataStoragePlain.bigData.size()));
        MLog.info(String.format("line:%d insert:%d update:%d delete:%d pkupdate:%d ",
                lineCount.get(), insertCount.get(), updateCount.get(), deleteCount.get(), pkUpdate.get()));
        MLog.info(String.format("bias count %d %d",
                bias1Count, bias2Count));
        //send a empty data
        for (BlockData blockData : blockDatas) {
            blockData.logQueue.put(LogBlock.EMPTY);
            //   blockData.buffQueue = null;
        }
        //
    }
}
