package org.pangolin.yx;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/6/4.
 */

//表元信息
class TableInfo {
    String scheme;
    String table;
    String pk;
    //列名
    ArrayList<String> columns = new ArrayList<>();
}

class LogColumnInfo {
    String name;
    int type;
    String oldValue;
    String newValue;
    int isPk;
}

class StringParser {
    String str;
    int off;

    StringParser(String s, int off) {
        this.str = s;
        this.off = off;
    }

    boolean end() {
        return off >= str.length();
    }
}


class AliLogData {
    //HashMap<String, TableInfo> tableInfos = new HashMap<>();
    TableInfo tableInfo;
    ArrayList<BlockLog> blockLogs = new ArrayList<>();
}

class BlockLog {
    //HashMap<String, TableInfo> tableInfos = new HashMap<>();
    TableInfo tableInfo = null;
    //HashMap<String, LogOfTable> logInfos = new HashMap<>();
    LogOfTable logOfTable = new LogOfTable();
    //FileBlock fileBlock;
    LogBlock logBlock;

    public void indexDone() {
        if (logOfTable != null) {
            logOfTable.flipBuffer();
        }
    }
}

class LogBlock {
    ArrayList<FileBlock> fileBlocks = new ArrayList<>();
    int index;
}

class FileBlock {
    String path;
    int index;
    long offsetInFile;
    long length;
    //整体off
    int offInBlock;
}


class LogRecord {
    //序列化区域
    //file info
    public int offsetInBlock;//这是在这个logblock中的偏移
    public int preLogOff;

    //非序列化
    public int length;
    public byte opType;
    public long preId;
    public long id;
    public long localOff = 0;//文件中的偏移
    public String logPath;//上一条关联日志在记录中的索引
    //public int preLogIndex = -1;
    //只有在rebuild时才会有数据
    public ArrayList<LogColumnInfo> columns = null;
}


public class LogParser {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    //private static ConcurrentLinkedQueue<FileBlock> fileBlocks = new ConcurrentLinkedQueue<>();
    private static ArrayList<LogBlock> logBlocks = new ArrayList<>();
    private static final AliLogData aliLogData = new AliLogData();
    private static AtomicInteger insertCount = new AtomicInteger();
    private static AtomicInteger updateCount = new AtomicInteger();
    private static AtomicInteger deleteCount = new AtomicInteger();
    private static ArrayList<String> filePathArray = new ArrayList<>();

    private static ArrayList<Long> fileLengths = new ArrayList<>();
    private static ArrayList<String> filePaths = new ArrayList<>();

    //根据offset 填写对应的文件和本地偏移
    public static void fillFileInfo(LogRecord logRecord, BlockLog blockLog) {
        long off = logRecord.offsetInBlock;
        LogBlock logBlock = blockLog.logBlock;
        for (int i = 0; i < logBlock.fileBlocks.size(); i++) {
            FileBlock fileBlock = logBlock.fileBlocks.get(i);
            if (off < fileBlock.offInBlock + fileBlock.length) {
                //in this file
                logRecord.logPath = fileBlock.path;
                logRecord.localOff = off - fileBlock.offInBlock + fileBlock.offsetInFile;
                break;
            }
        }
    }

    private static long allLogFileLength() {
        long len = 0;
        int fileIndex = 1;
        long totalLen = 0;
        while (true) {
            String path = Config.DATA_HOME + "/" + fileIndex + ".txt";
            File file = new File(path);
            if (file.exists()) {
                len += file.length();
                fileLengths.add(file.length());
                filePaths.add(path);
                totalLen += file.length();
            } else {
                break;
            }
            fileIndex++;
        }
        logger.info(String.format("file total len %d", totalLen));
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
                logger.info(String.format("last mod %d", file.lastModified()));

                long off = 0;
                while (off < file.length()) {
                    long mapLen = Math.min(blockLength - curLen, file.length() - off);
                    FileBlock newFileBlock = new FileBlock();
                    newFileBlock.path = path;
                    newFileBlock.length = mapLen;
                    newFileBlock.offsetInFile = off;
                    //只要每个block小于2G 这就是ok的
                    newFileBlock.offInBlock = (int) curLen;
                    logBlock.fileBlocks.add(newFileBlock);
                    //logBlock.fileBlockLength.add(mapLen);
                    //logBlock.filePaths.add(path);
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

    private static byte stringToOp(String s) throws Exception {

        if (s.equals("U")) {
            return Config.OP_TYPE_UPDATE;
        } else if (s.equals("I")) {
            return Config.OP_TYPE_INSERT;
        } else if (s.equals("D")) {
            return Config.OP_TYPE_DELETE;
        } else {
            throw new Exception("unknown op " + s);
        }
    }

    private static void parseLine(ReadLineInfo lineInfo, BlockLog blockLog, FileBlock fileBlock) throws Exception {
        String line = lineInfo.line;
        StringParser parser = new StringParser(line, 0);
        String uid = Util.getNextToken(parser, '|');
        String time = Util.getNextToken(parser, '|');
        String scheme = Util.getNextToken(parser, '|');

        /*
        if (!scheme.equals("middleware5")) {
            logger.info("ERROR ", scheme);
            System.exit(0);
        }
        */

        String table = Util.getNextToken(parser, '|');
        if (!scheme.equals(Config.queryData.scheme)) {
            return;
        }

        if (!table.equals(Config.queryData.table)) {
            return;
        }

        String op = Util.getNextToken(parser, '|');

        //table的第一条insert记录包含所有列, 我们记录下元信息
        if (op.equals("I")) {
            if (blockLog.tableInfo == null) {
                int off = parser.off;
                TableInfo info = new TableInfo();
                info.scheme = scheme;
                info.table = table;
                LogColumnInfo cinfo = Util.getNextColumnInfo(parser);
                while (cinfo != null) {
                    info.columns.add(cinfo.name);
                    if (cinfo.isPk == 1) {
                        info.pk = cinfo.name;
                    }
                    cinfo = Util.getNextColumnInfo(parser);
                }
                blockLog.tableInfo = info;
                parser.off = off;
            }
        }

        //解析到主键为止
        LogColumnInfo cinfo = Util.getNextColumnInfo(parser);
        while (cinfo != null) {
            if (cinfo.isPk == 1) {
                break;
            }
            cinfo = Util.getNextColumnInfo(parser);
        }

        if (cinfo == null) {
            throw new Exception("no pk");
        }
        //build logRecord
        LogRecord linfo = new LogRecord();

        linfo.opType = stringToOp(op);
        //linfo.logPath = blockLog.fileBlock.path;
        linfo.offsetInBlock = (int) (lineInfo.off + fileBlock.offInBlock - fileBlock.offsetInFile);
        linfo.length = lineInfo.length;
        if (op.equals("U")) {
            linfo.id = Long.parseLong(cinfo.newValue);
            linfo.preId = Long.parseLong(cinfo.oldValue);
            if (updateCount.incrementAndGet() == 1) {
                //打印第一条update
                logger.info(lineInfo.line);
            }

        } else if (op.equals("I")) {
            linfo.id = Long.parseLong(cinfo.newValue);
            insertCount.incrementAndGet();
        } else if (op.equals("D")) {
            linfo.preId = Long.parseLong(cinfo.oldValue);
            deleteCount.incrementAndGet();
        } else {
            throw new Exception("非法的操作类型");
        }
        blockLog.logOfTable.putLogInBuff(linfo);
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
                }
                synchronized (aliLogData) {
                    aliLogData.blockLogs.add(blockLog);
                }
                blockLog.indexDone();
                latch.countDown();
                logger.info("worker done");

            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }

    private static CountDownLatch latch;


    private static void parseLogBlock(FileBlock block, BlockLog blockLog) throws Exception {
        LineReader lineReader = new LineReader(block.path, block.offsetInFile, block.length);
        ReadLineInfo line = lineReader.readLine();
        int lineIndex = 1;
        while (line.line != null) {
            Util.parseLogCount.incrementAndGet();
            parseLine(line, blockLog, block);
            line = lineReader.readLine();
            lineIndex++;
        }
    }

    private static void sortBlock() {
        //get table Info
        for (BlockLog blockLog : aliLogData.blockLogs) {
            TableInfo v = blockLog.tableInfo;
            if (v != null) {
                aliLogData.tableInfo = v;
            }
        }
        //sort by index
        Collections.sort(aliLogData.blockLogs, new Comparator<BlockLog>() {
            @Override
            public int compare(BlockLog o1, BlockLog o2) {
                return o1.logBlock.index - o2.logBlock.index;
            }
        });
    }

    public static AliLogData parseLog() throws Exception {

        splitLogFile(Config.CPU_COUNT);

        latch = new CountDownLatch(logBlocks.size());

        for (int i = 0; i < logBlocks.size(); i++) {
            Thread th = new Thread(new Worker(logBlocks.get(i)));
            th.start();
        }
        latch.await();
        sortBlock();
        return aliLogData;

    }
}
