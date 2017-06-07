package org.pangolin.xuzhe;

/**
 * Created by ubuntu on 17-6-6.
 */
public class Column {
    public final String name;
    public final char type;
    public final boolean isPK;
    public Column(String name, char type, boolean isPK) {
        this.name = name;
        this.type = type;
        this.isPK = isPK;
    }

    /**
     * input example :first_name:2:0
     * @param str
     * @return
     */
    public static Column parser(String str) {
        int index = str.indexOf(':');
        String name = str.substring(0, index);
        char type = str.charAt(index+1);
        boolean isPK = str.charAt(index+3) == '0' ? false : true;
        return new Column(name, type, isPK);
    }
}
