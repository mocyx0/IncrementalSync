package org.pangolin.xuzhe.positiveorder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by 29146 on 2017/6/16.
 */
public class Record {
    private Long pk; //主键值
    private List<ByteBuffer> columnValue;
    public Record(Long pk,int columnSize){
        this.pk = pk;
        this.columnValue = new ArrayList<>(columnSize);
    }

    public void setPk(Long pk) {
        this.pk = pk;
    }

    public Long getPk() {
        return pk;
    }


    public List<ByteBuffer> getColumnValue() {
        return columnValue;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(100);
        sb.append(pk);
        for(ByteBuffer buffer : columnValue) {
            sb.append('\t').append(new String(buffer.array(), 0, buffer.limit()));
        }
        return sb.toString();
    }
}
