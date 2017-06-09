package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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


    private static ConcurrentLinkedQueue<FileBlock> fileBlocks = new ConcurrentLinkedQueue<>();
    private static final AliLogData aliLogData = new AliLogData();
    private static AtomicInteger insertCount = new AtomicInteger();
    private static AtomicInteger updateCount = new AtomicInteger();
    private static AtomicInteger deleteCount = new AtomicInteger();
    private static AtomicInteger lineCount = new AtomicInteger();

    private static ArrayList<String> filePathArray = new ArrayList<>();

    private static ConcurrentHashMap<String, OpCount> tableOpCount = new ConcurrentHashMap<>();


    private static void splitLogFile() {
        int fileIndex = 1;
        int blockIndex = 0;
        while (true) {
            String path = Config.DATA_HOME + "/" + fileIndex + ".txt";
            File file = new File(path);
            if (file.exists()) {
                long off = 0;
                while (off < file.length()) {
                    FileBlock newBlock = new FileBlock();
                    newBlock.path = path;
                    newBlock.index = blockIndex++;
                    newBlock.length = (int) Math.min(Config.BLOCK_SIZE, file.length() - off);
                    newBlock.offset = off;
                    off += Config.BLOCK_SIZE;
                    fileBlocks.add(newBlock);
                }
                fileIndex++;
            } else {
                break;
            }
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
        @Override
        public void run() {
            try {
                while (true) {
                    FileBlock block = fileBlocks.poll();
                    if (block == null) {
                        break;
                    } else {
                        BlockLog blockLog = parseLogBlock(block);
                        synchronized (aliLogData) {
                            aliLogData.blockLogs.add(blockLog);
                        }
                        //logger.info(String.format("Worker parse done, file: %s index: %d", block.path, block.index));
                        latch.countDown();
                    }
                }
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

    private static BlockLog parseLogBlock(FileBlock block) throws Exception {
        BlockLog blockLog = new BlockLog();
        blockLog.fileBlock = block;
        LineReader lineReader = new LineReader(block.path, block.offset, block.length);


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
        splitLogFile();
        latch = new CountDownLatch(fileBlocks.size());
        int cpu = Runtime.getRuntime().availableProcessors();
        cpu = 1;
        for (int i = 0; i < cpu; i++) {
            Thread th = new Thread(new Worker());
            th.start();
        }
        latch.await();
        long t2 = System.currentTimeMillis();
        logger.info("parser test done ,cost time", t2 - t1);
        logger.info(String.format("line:%d update:%d insert:%d delete:%d ", lineCount.get(), updateCount.get(), insertCount.get(), deleteCount.get()));
        for (String s : tableOpCount.keySet()) {
            OpCount opCount = tableOpCount.get(s);
            logger.info(String.format("table %s line:%d update:%d insert:%d delete:%d ", s, opCount.lineCount.get(), opCount.updateCount.get(), opCount.insertCount.get(), deleteCount.get()));
        }
        return aliLogData;

    }
}
