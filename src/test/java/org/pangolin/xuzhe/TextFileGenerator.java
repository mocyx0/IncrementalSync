package org.pangolin.xuzhe;

import io.netty.buffer.ByteBuf;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by ubuntu on 17-6-3.
 */
public class TextFileGenerator {
    private final int FILE_SIZE = 1<<30;
    private final int LINE_LENGTH = 50;
    private final int LINE_LENGTH_WITHOUT_LINE_BREAK = LINE_LENGTH-1;
    private final static byte[] line = "000001:106|1489133349000|test|user|I|id:1:1|<NULL>|102|name:2:0|<NULL>|ljh|score:1:0|<NULL>|98|\n".getBytes();
    @Test
    public void test() throws IOException {
        File file = new File("data/1.txt");
        file.getParentFile().mkdirs();
        ByteBuffer buffer = ByteBuffer.allocateDirect(1<<15);

        FileChannel channel = new FileOutputStream(file).getChannel();
        for(int i = 0; i < FILE_SIZE;) {
            if(fillOneLine(buffer)) {
                i += LINE_LENGTH;
            } else {
                buffer.flip();
                channel.write(buffer);
                buffer.clear();
            }
        }
        channel.close();
    }

    private boolean fillOneLine(ByteBuffer buffer) {
        if(buffer.remaining() < 50) return false;
        buffer.put(line);

        return true;
    }
}
