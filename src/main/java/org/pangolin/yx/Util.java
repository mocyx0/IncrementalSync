package org.pangolin.yx;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/6/4.
 */
public class Util {

    private static int MAX_LINE_SIZE = 2048;

    public static AtomicInteger readLogCount = new AtomicInteger();
    public static AtomicInteger parseLogCount = new AtomicInteger();
    private static ThreadLocal<byte[]> readBuffers = new ThreadLocal<>();

    private static byte[] getReadBuffer() {
        byte[] re = readBuffers.get();
        if (re == null) {
            readBuffers.set(new byte[MAX_LINE_SIZE]);
            re = readBuffers.get();
        }
        return re;
    }

    public static String fillLogData(RandomAccessFile raf, LogRecord log) throws Exception {

        byte[] buffer = getReadBuffer();
        if (log.offsetInBlock < 0) {
            System.out.print(1);
        }
        raf.seek(log.localOff);
        raf.read(buffer, 0, buffer.length);
        int l = 0;
        for (int i = 0; i < buffer.length; i++) {
            if (buffer[i] == '\n') {
                l = i;
                break;
            }
        }
        if (l == 0) {
            throw new Exception("line length > " + MAX_LINE_SIZE);
        }
        log.length = l;

        String line = new String(buffer, 0, l);

        StringParser parser = new StringParser(line, 0);
        String uid = Util.getNextToken(parser, '|');
        String time = Util.getNextToken(parser, '|');
        String scheme = Util.getNextToken(parser, '|');
        String table = Util.getNextToken(parser, '|');
        String op = Util.getNextToken(parser, '|');

        log.columns = new ArrayList<>();
        //解析到主键为止
        LogColumnInfo cinfo = Util.getNextColumnInfo(parser);
        while (cinfo != null) {
            log.columns.add(cinfo);
            if (cinfo.isPk == 1) {
                if (op.equals("U")) {
                    log.id = Long.parseLong(cinfo.newValue);
                    log.preId = Long.parseLong(cinfo.oldValue);
                } else if (op.equals("I")) {
                    log.id = Long.parseLong(cinfo.newValue);
                    log.preId = -1;
                } else {
                    throw new Exception("wrong op type");
                }

            }
            cinfo = Util.getNextColumnInfo(parser);
        }
        //done
        readLogCount.incrementAndGet();
        return line;
    }

    public static LogColumnInfo getNextColumnInfo(StringParser parser) {
        if (parser.end()) {
            return null;
        }
        LogColumnInfo info = new LogColumnInfo();
        info.name = getNextToken(parser, ':');
        info.type = Integer.parseInt(getNextToken(parser, ':'));
        info.isPk = Integer.parseInt(getNextToken(parser, '|'));
        info.oldValue = getNextToken(parser, '|');
        info.newValue = getNextToken(parser, '|');
        return info;
    }

    public static String getNextToken(StringParser parser, char delimit) {
        int s = parser.off;
        while (s < parser.str.length() && parser.str.charAt(s) == delimit) {
            s++;
        }
        int e = s;
        while (e < parser.str.length() && parser.str.charAt(e) != delimit) {
            e++;
        }
        parser.off = e + 1;
        return parser.str.substring(s, e);

    }

    public static ArrayList<String> logFiles(String dir) {
        ArrayList<String> files = new ArrayList<>();
        for (int i = 1; i < 100; i++) {
            String filepath = dir + "/" + i + ".txt";
            File f = new File(filepath);
            if (f.exists()) {
                files.add(filepath);
            } else {
                break;
            }
        }
        return files;
    }

    public static ArrayList<MappedByteBuffer> mapFile(RandomAccessFile raf, FileChannel.MapMode mode) throws Exception {
        int mmapSize = Integer.MAX_VALUE;
        ArrayList<MappedByteBuffer> mmaps = new ArrayList<>();
        long totalSize = raf.length();
        long mapLen = 0;
        while (mapLen < totalSize) {
            int len = (int) Math.min(mmapSize, totalSize - mapLen);
            MappedByteBuffer mmap = raf.getChannel().map(mode, mapLen, len);
            mmaps.add(mmap);
            mapLen += len;
        }
        return mmaps;
    }

}


