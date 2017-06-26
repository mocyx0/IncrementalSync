package org.pangolin.xuzhe.reformat;

import java.io.FileOutputStream;

import static org.pangolin.xuzhe.reformat.Constants.PARSER_NUM;

/**
 * Created by XuZhe on 2017/6/23.
 */
public class WriterThread extends Thread {
    Parser[] parsers;
    public WriterThread(Parser[] parsers) {
        setName("Writer");
        this.parsers = parsers;
    }

    @Override
    public void run() {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream("other/middle.dat2");
            int count = 0;
            int emptyCount = 0;
            byte[] intBytes = new byte[4];
            while(true) {
                int parserIndex = count % PARSER_NUM;
                ++count;
                byte[] data = parsers[parserIndex].compressedBytesBlockingQueue.take();
                if(data.length == 0) {
                    ++emptyCount;
                    System.out.printf("Parser:%d finished!\n", parserIndex);
                    if(emptyCount == PARSER_NUM) {
                        break;
                    }
                    continue;
                }
//                byte[] result = new byte[1<<20];
//                Redo.uncompress(data, 0, data.length, result, 0, result.length);
                intBytes[0] = (byte)(data.length>>24);
                intBytes[1] = (byte)(data.length>>16);
                intBytes[2] = (byte)(data.length>>8 );
                intBytes[3] = (byte)(data.length    );
                fileOutputStream.write(intBytes);
                fileOutputStream.write(data);
            }
            fileOutputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
