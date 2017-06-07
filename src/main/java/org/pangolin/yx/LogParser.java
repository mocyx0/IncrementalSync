package org.pangolin.yx;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
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

class LogOfTable {
    private ArrayList<LogRecord> logArray = new ArrayList<>();//log链表
    private HashMap<Long, Integer> logPos = new HashMap<>();//当前的id对应的上一条log索引

    public LogRecord getLog(int index) {
        return logArray.get(index);
    }


    public boolean isDeleted(long id) {
        if (logPos.containsKey(id) && logPos.get(id) == -1) {
            return true;
        } else {
            return false;
        }
    }

    public LogRecord getLogById(Long id) {
        if (logPos.containsKey(id)) {
            return logArray.get(logPos.get(id));
        } else {
            return null;
        }
    }

    public void putLog(LogRecord record) {
        if (record.preId == 4996) {
            System.out.print(1);
        }
        if (record.opType == Config.OP_TYPE_UPDATE) {
            record.preLogIndex = getPreLogIndex(record.preId);
            if (record.preId != record.id) {
                logPos.put(record.preId, -1);
            }
            logArray.add(record);
            logPos.put(record.id, logArray.size() - 1);


        } else if (record.opType == Config.OP_TYPE_INSERT) {
            logArray.add(record);
            logPos.put(record.id, logArray.size() - 1);
        } else if (record.opType == Config.OP_TYPE_DELETE) {
            logPos.put(record.preId, -1);
        }
/*

        if (record.opType != Config.OP_TYPE_INSERT) {
            record.preLogIndex = getPreLogIndex(record.preId);
        }
        //主键update
        if (record.opType == Config.OP_TYPE_UPDATE && (record.preId != record.id)) {
            //delete old
            //logPos.remove(record.preId);
            logPos.put(record.preId, -1);
        }

        if (record.opType != Config.OP_TYPE_DELETE) {
            record.preLogIndex = getPreLogIndex(record.preId);
            logArray.add(record);
            logPos.put(record.id, logArray.size() - 1);
        } else {
            //delete op
            //logPos.remove(record.preId);
            logPos.put(record.preId, -1);
        }
        */
    }

    public int getPreLogIndex(Long id) {
        if (logPos.containsKey(id)) {
            return logPos.get(id);
        } else {
            return -1;
        }
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
    FileBlock fileBlock;
}

class FileBlock {
    String path;
    int index;
    long offset;
    int length;
}

class LogRecord {
    //
    //public String opType;
    public byte opType;
    public long preId = -2;
    public long id = -2;
    //file info
    public long offset;
    public String logPath;
    public int length;
    //上一条关联日志在记录中的索引
    public int preLogIndex = -1;
    //只有在rebuild时才会有数据
    public ArrayList<LogColumnInfo> columns = null;
}


public class LogParser {

    private static ConcurrentLinkedQueue<FileBlock> fileBlocks = new ConcurrentLinkedQueue<>();
    private static final AliLogData aliLogData = new AliLogData();
    private static AtomicInteger insertCount = new AtomicInteger();
    private static AtomicInteger updateCount = new AtomicInteger();
    private static AtomicInteger deleteCount = new AtomicInteger();

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

    private static void parseLine(ReadLineInfo lineInfo, BlockLog blockLog) throws Exception {
        String line = lineInfo.line;


        StringParser parser = new StringParser(line, 0);
        String uid = Util.getNextToken(parser, '|');
        String time = Util.getNextToken(parser, '|');
        String scheme = Util.getNextToken(parser, '|');
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
        linfo.logPath = blockLog.fileBlock.path;
        linfo.offset = lineInfo.off;
        linfo.length = lineInfo.length;
        if (op.equals("U")) {
            linfo.id = Long.parseLong(cinfo.newValue);
            linfo.preId = Long.parseLong(cinfo.oldValue);
            updateCount.incrementAndGet();
        } else if (op.equals("I")) {
            linfo.id = Long.parseLong(cinfo.newValue);
            insertCount.incrementAndGet();
        } else if (op.equals("D")) {
            linfo.preId = Long.parseLong(cinfo.oldValue);
            deleteCount.incrementAndGet();
        } else {
            throw new Exception("非法的操作类型");
        }
        blockLog.logOfTable.putLog(linfo);
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
                        latch.countDown();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }

    private static CountDownLatch latch;


    private static BlockLog parseLogBlock(FileBlock block) throws Exception {
        BlockLog blockLog = new BlockLog();
        blockLog.fileBlock = block;
        LineReader lineReader = new LineReader(block.path, block.offset, block.length);
        ReadLineInfo line = lineReader.readLine();
        int lineIndex = 1;
        while (line.line != null) {
            parseLine(line, blockLog);
            line = lineReader.readLine();
            lineIndex++;
        }
        return blockLog;
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
                return o1.fileBlock.index - o2.fileBlock.index;
            }
        });
    }

    public static AliLogData parseLog() throws Exception {

        splitLogFile();
        latch = new CountDownLatch(fileBlocks.size());
        int cpu = Runtime.getRuntime().availableProcessors();
        cpu = 1;
        for (int i = 0; i < cpu; i++) {
            Thread th = new Thread(new Worker());
            th.start();
        }
        latch.await();
        sortBlock();
        //

        return aliLogData;
//        logPath = Config.DATA_HOME + "/1.txt";
//        File f1 = new File(logPath);
//        lineReader = new LineReader(logPath, 0, f1.length());


    }
}
