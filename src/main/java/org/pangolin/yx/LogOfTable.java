package org.pangolin.yx;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/6/7.
 * 存储格式:
 * 1 Delete   不存储
 * 2 Update   byte type | long preid  | long id | int fileOff | int length | int preLogOff
 * 3 Insert   byte type | long id | int fileOff | int length|int preLogOff
 */


class LogOfTable {
    public static AtomicInteger TOTAL_MEM = new AtomicInteger();

    private static int LOG_TYPE_UPDATE = 1;
    private static int LOG_TYPE_INSERT = 2;

    //private ArrayList<LogRecord> logArray = new ArrayList<>();//log链表
    // private HashMap<Long, Integer> logPos = new HashMap<>();//当前的id对应的上一条log索引
    //    private HashMap<Long, Integer> logOff = new HashMap<>();
    private LinearHashing logOff = new LinearHashing();

    private int writeOff;
    private static int BUFFER_SIZE = 1024 * 1024;
    private int bufferTotalLength = 0;
    private ArrayList<ByteBuffer> datas = new ArrayList<>();

    LogOfTable() {
        datas.add(ByteBuffer.allocate(BUFFER_SIZE));
        TOTAL_MEM.addAndGet(BUFFER_SIZE);
    }

    public void flipBuffer() {
        for (ByteBuffer buffer : datas) {
            buffer.flip();
        }
    }

    /*
    public LogRecord getLog(int index) {
        return logArray.get(index);
    }
    */

    private void putToBufferRaw(ByteBuffer buffer, LogRecord record) throws Exception {
        buffer.putInt(record.offsetInBlock);
        //buffer.putInt(record.length);
        buffer.putInt(record.preLogOff);
        /*
        buffer.put(record.opType);
        if (record.opType == Config.OP_TYPE_UPDATE) {
            buffer.putLong(record.preId);
            buffer.putLong(record.id);
            buffer.putLong(record.offsetInFile);
            buffer.putInt(record.length);
            buffer.putInt(record.preLogOff);

        } else if (record.opType == Config.OP_TYPE_INSERT) {
            buffer.putLong(record.id);
            buffer.putLong(record.offsetInFile);
            buffer.putInt(record.length);
            //buffer.putInt(record.preLogOff);
        } else {
            throw new Exception("error");
        }
        */
    }

    private LogRecord readFromBuffer(ByteBuffer buffer) throws Exception {
        LogRecord record = new LogRecord();
        record.offsetInBlock = buffer.getInt();
        //record.length = buffer.getInt();
        record.preLogOff = buffer.getInt();
        return record;

        /*
        record.opType = buffer.get();
        if (record.opType == Config.OP_TYPE_UPDATE) {
            record.preId = buffer.getLong();
            record.id = buffer.getLong();
            record.offsetInFile = buffer.getLong();
            record.length = buffer.getInt();
            record.preLogOff = buffer.getInt();
        } else if (record.opType == Config.OP_TYPE_INSERT) {
            record.preId = -1;
            record.id = buffer.getLong();
            record.offsetInFile = buffer.getLong();
            record.length = buffer.getInt();
            record.preLogOff = -1;
        } else {
            throw new Exception();
        }
        return record;
        */
    }

    public synchronized LogRecord getLog(int off) throws Exception {
        int index = off / BUFFER_SIZE;
        int bufferOff = off % BUFFER_SIZE;
        ByteBuffer buffer = datas.get(index);
        buffer.position(bufferOff);
        return readFromBuffer(buffer);
    }
    /*
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
    */

    public boolean isDeleted(long id) throws Exception {
        if (logOff.containsKey(id) && logOff.get(id) == -1) {
            return true;
        } else {
            return false;
        }
    }

    public LogRecord getLogById(long id) throws Exception {
        if (logOff.containsKey(id)) {
            //return logArray.get(logOff.get(id));
            int off = logOff.get(id);
            LogRecord record = getLog(off);

            return record;
        } else {
            return null;
        }
    }

    public int getPreLogOff(Long id) throws Exception {
        if (logOff.containsKey(id)) {
            return logOff.get(id);
        } else {
            return -1;
        }
    }


    private void newBufferBlock() {
        //writeOff += datas.get(datas.size() - 1).position();
        writeOff += BUFFER_SIZE;
        datas.add(ByteBuffer.allocate(BUFFER_SIZE));
        TOTAL_MEM.addAndGet(BUFFER_SIZE);
    }

    //return write postion
    private int putToBuffer(LogRecord record) throws Exception {
        ByteBuffer buffer = datas.get(datas.size() - 1);
        int pos = buffer.position();
        int re = 0;
        try {
            putToBufferRaw(buffer, record);
            re = writeOff + pos;
        } catch (BufferOverflowException e) {
            buffer.position(pos);
            newBufferBlock();
            buffer = datas.get(datas.size() - 1);
            putToBufferRaw(buffer, record);
            re = writeOff;//从新的地址开始
        }
        return re;
    }

    public void putLogInBuff(LogRecord record) throws Exception {

        if (record.opType == Config.OP_TYPE_UPDATE) {
            //pre off
            record.preLogOff = getPreLogOff(record.preId);
            //
            if (record.preId != record.id) {
                logOff.put(record.preId, -1);
            }
            //
            //logArray.add(record);
            //logOff.put(record.id, logArray.size() - 1);
            int off = putToBuffer(record);
            logOff.put(record.id, off);
        } else if (record.opType == Config.OP_TYPE_INSERT) {
            //logArray.add(record);
            //logPos.put(record.id, logArray.size() - 1);
            record.preLogOff = -1;
            int off = putToBuffer(record);
            logOff.put(record.id, off);
        } else if (record.opType == Config.OP_TYPE_DELETE) {
            //logPos.put(record.preId, -1);
            logOff.put(record.preId, -1);
        }
    }

    /*
    public void putLog(LogRecord record) {
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
    }
    public int getPreLogIndex(Long id) {
        if (logPos.containsKey(id)) {
            return logPos.get(id);
        } else {
            return -1;
        }
    }
    */


}
