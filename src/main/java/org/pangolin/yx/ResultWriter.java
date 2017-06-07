package org.pangolin.yx;

import com.sun.xml.internal.fastinfoset.tools.FI_DOM_Or_XML_DOM_SAX_SAXEvent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/5.
 */

public class ResultWriter {
    public static ByteBuffer writeToBuffer(RebuildResult result) {
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024 * 4);
        for (ArrayList<String> strs : result.datas) {
            for (int i = 0; i < strs.size(); i++) {
                buffer.put(strs.get(i).getBytes());
                if (i != strs.size() - 1) {
                    buffer.put((byte) '\t');
                }
            }
            buffer.put((byte) '\t');
        }
        return buffer;
    }

    public static void writeToFile(RebuildResult result) throws Exception {


        String path = Config.RESULT_HOME + "/" + Config.RESULT_NAME;
        File f = new File(path);
        if (f.exists()) {
            f.delete();
        }

        RandomAccessFile raf = new RandomAccessFile(path, "rw");
        BufferedWriter writer = new BufferedWriter(new FileWriter(raf.getFD()));
        for (ArrayList<String> strs : result.datas) {
            for (int i = 0; i < strs.size(); i++) {
                writer.write(strs.get(i));
                if (i != strs.size() - 1) {
                    writer.write('\t');
                }
            }
            writer.write('\n');
        }
        writer.close();
    }
}
