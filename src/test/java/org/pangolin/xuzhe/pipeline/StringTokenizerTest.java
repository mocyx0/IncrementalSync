package org.pangolin.xuzhe.pipeline;

import org.junit.Test;

/**
 * Created by ubuntu on 17-6-14.
 */
public class StringTokenizerTest {
    @Test
    public void test() {
        String[] result = new String[30];
        String str = "|mysql-bin.00001717148759|1496736165000|middleware3|student|I|id:1:1|NULL|1|first_name:2:0|NULL|徐|last_name:2:0|NULL|依|sex:2:0|NULL|男|score:1:0|NULL|66|";
        int cnt = StringTokenizer.tokenize(str, '|', result);
        for(int i = 0; i < cnt; i++) {
            System.out.println(result[i]);
        }
    }
}
