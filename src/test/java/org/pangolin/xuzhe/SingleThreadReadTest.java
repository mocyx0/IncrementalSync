package org.pangolin.xuzhe;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by ubuntu on 17-6-3.
 */
public class SingleThreadReadTest {

    @Test
    public void test() throws IOException {
        File file = new File("data/1.txt");
        if(!file.exists()) Assert.assertFalse(file + "is not exist!", false);
        FileChannel fileChannel = new FileInputStream(file).getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(1<<20);
        long size = fileChannel.size();
        while(fileChannel.position() < size) {
            fileChannel.read(buffer);
            buffer.flip();
        }

    }
}
