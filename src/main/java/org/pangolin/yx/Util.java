package org.pangolin.yx;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

/**
 * Created by yangxiao on 2017/6/4.
 */
public class Util {


    public static void fillLogData(RandomAccessFile raf, LogInfo log) throws Exception {
        byte[] buffer = new byte[log.length];
        raf.seek(log.offset);
        raf.read(buffer, 0, buffer.length);
        String line = new String(buffer);
        StringParser parser = new StringParser(line, 0);
        String uid = Util.getNextToken(parser, '|');
        String time = Util.getNextToken(parser, '|');
        String scheme = Util.getNextToken(parser, '|');
        String table = Util.getNextToken(parser, '|');
        String op = Util.getNextToken(parser, '|');

        log.columns=new ArrayList<>();
        //解析到主键为止
        ParserColumnInfo cinfo = Util.getNextColumnInfo(parser);
        while (cinfo != null) {
            log.columns.add(cinfo);
            cinfo = Util.getNextColumnInfo(parser);
        }
        //done
    }

    public static ParserColumnInfo getNextColumnInfo(StringParser parser) {
        if (parser.end()) {
            return null;
        }
        ParserColumnInfo info = new ParserColumnInfo();
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


}


