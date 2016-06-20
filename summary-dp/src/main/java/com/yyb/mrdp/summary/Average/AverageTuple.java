package com.yyb.mrdp.summary.average;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Administrator
 *         2016/6/19.
 */
public class AverageTuple implements Writable{

    private long count = 0;
    private float average = 0;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public float getAverage() {
        return average;
    }

    public void setAverage(float average) {
        this.average = average;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        out.writeFloat(average);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readLong();
        average = in.readFloat();
    }

    @Override
    public String toString() {
        return count + "\t" + average;
    }
}
