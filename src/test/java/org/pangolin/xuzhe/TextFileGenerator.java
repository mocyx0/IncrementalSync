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
        for(int j = 0; j < LINE_LENGTH_WITHOUT_LINE_BREAK; j++)
            buffer.put((byte)('a' + (j%26)));
        buffer.put((byte)'\n');
        return true;
    }
}
