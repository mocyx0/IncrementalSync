package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/6/8.
 */

public class LogParserTest {
    private static class OpCount {
        AtomicInteger insertCount = new AtomicInteger();
        AtomicInteger updateCount = new AtomicInteger();
        AtomicInteger deleteCount = new AtomicInteger();
        AtomicInteger lineCount = new AtomicInteger();
    }


    //private static ConcurrentLinkedQueue<FileBlock> fileBlocks = new ConcurrentLinkedQueue<>();
    private static ArrayList<LogBlock> logBlocks = new ArrayList<>();
    private static final AliLogData aliLogData = new AliLogData();
    private static AtomicInteger insertCount = new AtomicInteger();
    private static AtomicInteger updateCount = new AtomicInteger();
    private static AtomicInteger deleteCount = new AtomicInteger();
    private static AtomicInteger lineCount = new AtomicInteger();

    private static ArrayList<String> filePathArray = new ArrayList<>();

    private static ConcurrentHashMap<String, OpCount> tableOpCount = new ConcurrentHashMap<>();

    private static long allLogFileLength() {
        long len = 0;
        int fileIndex = 1;
        while (true) {
            String path = Config.DATA_HOME + "/" + fileIndex + ".txt";
            File file = new File(path);
            if (file.exists()) {
                len += file.length();
            } else {
                break;
            }
            fileIndex++;
        }
        return len;
    }

    private static void splitLogFile(int n) {
        long totalLength = allLogFileLength();
        long blockLength = totalLength / n;

        int fileIndex = 1;
        int blockIndex = 0;
        long curLen = 0;
        LogBlock logBlock = new LogBlock();
        logBlock.index = blockIndex;
        while (true) {
            String path = Config.DATA_HOME + "/" + fileIndex + ".txt";
            File file = new File(path);
            if (file.exists()) {
                long off = 0;
                while (off < file.length()) {
                    long mapLen = Math.min(blockLength - curLen, file.length() - off);
                    FileBlock newFileBlock = new FileBlock();
                    newFileBlock.path = path;
                    newFileBlock.length = mapLen;
                    newFileBlock.offsetInFile = off;
                    newFileBlock.offInBlock = (int) curLen;
                    logBlock.fileBlocks.add(newFileBlock);
                    curLen += mapLen;
                    off += mapLen;
                    if (curLen == blockLength) {
                        //new block
                        logBlocks.add(logBlock);
                        blockIndex++;
                        logBlock = new LogBlock();
                        logBlock.index = blockIndex;
                        curLen = 0;
                    }
                }
            } else {
                break;
            }
            fileIndex++;
        }
    }


    private static void parseLine(ReadLineInfo lineInfo, BlockLog blockLog) throws Exception {
        String line = lineInfo.line;
        StringParser parser = new StringParser(line, 0);
        String uid = Util.getNextToken(parser, '|');
        String time = Util.getNextToken(parser, '|');
        String scheme = Util.getNextToken(parser, '|');
        String table = Util.getNextToken(parser, '|');
        String op = Util.getNextToken(parser, '|');

        String hashKey = scheme + " " + table;
        if (!tableOpCount.containsKey(hashKey)) {
            tableOpCount.putIfAbsent(hashKey, new OpCount());
        }
        OpCount opCount = tableOpCount.get(hashKey);
        opCount.lineCount.incrementAndGet();
        lineCount.incrementAndGet();
        if (op.equals("U")) {
            opCount.updateCount.incrementAndGet();
            updateCount.incrementAndGet();
        } else if (op.equals("I")) {

            opCount.insertCount.incrementAndGet();
            insertCount.incrementAndGet();
        } else if (op.equals("D")) {
            opCount.deleteCount.incrementAndGet();
            deleteCount.incrementAndGet();
        }
    }

    private static class Worker implements Runnable {
        private LogBlock logBlock;

        Worker(LogBlock logBlock) {
            this.logBlock = logBlock;
        }

        @Override
        public void run() {
            try {
                BlockLog blockLog = new BlockLog();
                blockLog.logBlock = logBlock;
                for (FileBlock fb : logBlock.fileBlocks) {
                    parseLogBlock(fb, blockLog);
                    synchronized (aliLogData) {
                        aliLogData.blockLogs.add(blockLog);
                    }
                }
                latch.countDown();
                logger.info("worker done");

            } catch (Exception e) {
                logger.info("{}", e);
                System.exit(0);
            } catch (Error e) {
                logger.info("{}", e);
                logger.info(e.toString());
                throw e;
            }
        }
    }

    private static CountDownLatch latch;

    private static BlockLog parseLogBlock(FileBlock block, BlockLog blockLog) throws Exception {
        LineReader lineReader = new LineReader(block.path, block.offsetInFile, block.length);
        ReadLineInfo line = lineReader.readLine();
        int lineIndex = 0;
        try {

            while (line.line != null) {
                parseLine(line, blockLog);
                line = lineReader.readLine();
                lineIndex++;
            }
            blockLog.indexDone();
        } catch (Exception e) {
            logger.info(String.format("parseLogBlock error  index:%d line:%s", lineIndex, line.line));
            throw e;
        }

        return blockLog;
    }

    private static Logger logger;

    public static AliLogData parseLog() throws Exception {
        logger = LoggerFactory.getLogger(Server.class);
        long t1 = System.currentTimeMillis();
        splitLogFile(Config.CPU_COUNT);

        latch = new CountDownLatch(logBlocks.size());
        for (int i = 0; i < logBlocks.size(); i++) {
            Thread th = new Thread(new Worker(logBlocks.get(i)));
            th.start();
        }
        latch.await();

        long t2 = System.currentTimeMillis();
        logger.info(String.format("parser test done ,cost time %d", t2 - t1));
        logger.info(String.format("line:%d update:%d insert:%d delete:%d ", lineCount.get(), updateCount.get(), insertCount.get(), deleteCount.get()));
        for (String s : tableOpCount.keySet()) {
            OpCount opCount = tableOpCount.get(s);
            logger.info(String.format("table %s line:%d update:%d insert:%d delete:%d ", s, opCount.lineCount.get(), opCount.updateCount.get(), opCount.insertCount.get(), deleteCount.get()));
        }
        return aliLogData;

    }
}
