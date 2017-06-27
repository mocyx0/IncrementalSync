package org.pangolin.yx.zhengxu;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/16.
 */

class LineInfo {
    byte[] data;
}

public class LineReader {

    private static int BUFFER_SIZE = 1024 * 4;
    ArrayList<RandomAccessFile> rafs = new ArrayList<>();
    int fileIndex = 0;

    long offInFile = 0;
    byte[] buffer = new byte[BUFFER_SIZE];
    int bufferLimit = 0;
    int bufferReadPos = 0;

    public LineReader(ArrayList<String> paths) throws Exception {
        for (String s : paths) {
            RandomAccessFile raf = new RandomAccessFile(s, "r");
            rafs.add(raf);
        }

    }

    private void nextBlock() throws Exception {
        RandomAccessFile raf = rafs.get(fileIndex);
        raf.seek(offInFile);
        bufferLimit = raf.read(buffer, 0, buffer.length);
        bufferReadPos = 0;
    }


    private LineInfo tryReadLine() {
        int lineEnd = -1;
        int i = bufferReadPos;
        while (i < bufferLimit) {
            if (buffer[i] == '\n') {
                lineEnd = i;
                break;
            }
            i++;
        }
        if (lineEnd == -1) {
            return null;
        }
        LineInfo lineInfo = new LineInfo();
        int len = lineEnd - bufferReadPos;
        lineInfo.data = new byte[len];
        System.arraycopy(buffer, bufferReadPos, lineInfo.data, 0, len);
        offInFile += len + 1;
        bufferReadPos = lineEnd + 1;
        return lineInfo;
    }

    public LineInfo nextLine() throws Exception {

        LineInfo lineInfo = tryReadLine();
        if (lineInfo == null) {
            nextBlock();
            lineInfo = tryReadLine();
        }
        if (lineInfo == null) {
            fileIndex += 1;
            offInFile = 0;
            if (fileIndex >= rafs.size()) {
                return null;
            }
            nextBlock();
            lineInfo = tryReadLine();
        }

        return lineInfo;
    }


}
