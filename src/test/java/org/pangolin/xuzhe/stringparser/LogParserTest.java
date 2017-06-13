package org.pangolin.xuzhe.stringparser;

import org.junit.Test;

import java.util.ArrayList;
import java.util.StringTokenizer;

import static org.junit.Assert.*;
/**
 * Created by ubuntu on 17-6-7.
 */
public class LogParserTest {
    @Test
    public void testBasic() {
        String str = "|mysql-bin.00001717148759|1496736165000|middleware3|student|I|id:1:1|NULL|1|first_name:2:0|NULL|徐|last_name:2:0|NULL|依|sex:2:0|NULL|男|score:1:0|NULL|66|";
        ArrayList<String> items = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(str, "|", false);
        while (tokenizer.hasMoreElements()) {
            items.add(tokenizer.nextToken());
        }
        assertEquals(LogParser.getDatabaseName(items), "middleware3");
        assertEquals(LogParser.getTableName(items), "student");
        assertEquals(LogParser.getOpType(items), "I");
        assertEquals(LogParser.getColumnCount(items), 5);
        String[] expected1 = {
                "id:1:1",
                "first_name:2:0",
                "last_name:2:0",
                "sex:2:0",
                "score:1:0"
        };
        assertArrayEquals(LogParser.getAllColumn(items), expected1);
        String[] expected2 = {
                "score:1:0",
                "NULL",
                "66"
        };
        assertArrayEquals(LogParser.getColumnAllInfoByIndex(items, 4), expected2);
        String[] expected3 = {
                "score",
                "1",
                "0"
        };
        assertArrayEquals(LogParser.getColumnMetaInfo(expected2[0]), expected3);

    }
    @Test
    public void testParser() {
        String str = "|mysql-bin.00001717148759|1496736165000|middleware3|student|I|id:1:1|NULL|1|first_name:2:0|NULL|徐|last_name:2:0|NULL|依|sex:2:0|NULL|男|score:1:0|NULL|66|";
        ArrayList<String> buffer = new ArrayList<>();
        LogParser.parseToIndex(str, 1, 0, buffer);
        System.out.println(buffer);
    }
}
