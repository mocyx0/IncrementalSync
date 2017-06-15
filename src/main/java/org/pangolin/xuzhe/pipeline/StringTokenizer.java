package org.pangolin.xuzhe.pipeline;

/**
 * Created by ubuntu on 17-6-14.
 */
public class StringTokenizer {
    public static int tokenize(String string, char delimiter, String[] result) {
        for(int i = 0; i < result.length; i++) {
            result[i] = null;
        }
        int wordCount = 0;
        int i = 0;
        if(string.charAt(i) == delimiter) ++i;
        int j = string.indexOf(delimiter, i);
        while( j >= 0) {
            result[wordCount++] = string.substring(i, j);
            i = j + 1;
            j = string.indexOf(delimiter, i);
        }

//        result[wordCount++] = string.substring(i);
        return wordCount;
    }
}