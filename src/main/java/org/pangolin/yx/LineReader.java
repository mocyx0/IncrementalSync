package org.pangolin.yx;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by yangxiao on 2017/6/4.
 */


class ReadLineInfo {
    String line;
    int off;
    int length;
}

public class LineReader {
    static int BUFFER_SZIE = 8 * 1024;
    //close
    long fileOff;
    //open
    long maxOff;
    byte[] buffer;
    int pos;
    int limit;
    long curFilePos;
    RandomAccessFile raf;

    public LineReader(String path, long fileOff, long lenth) throws Exception {
        buffer = new byte[BUFFER_SZIE];
        this.maxOff = fileOff + lenth;
        this.fileOff = fileOff;
        curFilePos = fileOff;
        pos = 0;
        raf = new RandomAccessFile(path, "r");
        raf.seek(curFilePos);
        initForward();
    }

    //ignore one line
    void initForward() throws Exception {
        fillBuffer();
        if (fileOff != 0) {
            readLine();
        }
    }

    //TODO utf-8
    String tryReadLine() {
        lastStringPos = pos + curFilePos;
        int s = pos;
        int e = -1;
        while (pos < limit) {
            if (buffer[pos] == '\n') {
                e = pos;
                pos++;
                break;
            }
            pos++;
        }
        if (e == -1) {
            pos = s;//reset
            return null;
        } else {
            return new String(buffer, s, e - s);
        }
    }

    void fillBuffer() throws Exception {
        curFilePos += pos;
        long newOff = curFilePos;
        raf.seek(newOff);
        int size = raf.read(buffer, 0, buffer.length);
        limit = size;
        pos = 0;
    }


    private boolean readDone = false;
    private long lastStringPos = 0;

    long getLastPos() {
        return lastStringPos;
    }

    ReadLineInfo readLine() throws Exception {

        if (readDone) {
            return new ReadLineInfo();
        }
        ReadLineInfo re = new ReadLineInfo();
        String s = tryReadLine();
        if (s == null) {
            fillBuffer();
            s = tryReadLine();
        }
        re.line = s;
        re.off = (int) lastStringPos;
        //-1 \n
        re.length = (int) (curFilePos + pos - lastStringPos - 1);

        //should be >
        if (curFilePos + pos > maxOff) {
            readDone = true;
        }
        return re;
    }

}
