package org.pangolin.yx;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/5.
 */


public class ResultWriter {

    public static void writeToFile(RebuildResult result) throws Exception {
        String path = Config.RESULT_HOME + "/" + Config.RESULT_NAME;
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
