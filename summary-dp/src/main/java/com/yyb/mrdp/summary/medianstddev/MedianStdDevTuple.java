package com.yyb.mrdp.summary.medianstddev;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Administrator
 *         2016/6/20.
 */
public class MedianStdDevTuple implements Writable{

    private float median = 0;
    private float stdDev = 0;

    public float getStdDev() {
        return stdDev;
    }

    public void setStdDev(float stdDev) {
        this.stdDev = stdDev;
    }

    public float getMedian() {
        return median;
    }

    public void setMedian(float median) {
        this.median = median;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(this.median);
        out.writeFloat(this.stdDev);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.median = in.readFloat();
        this.stdDev = in.readFloat();

    }

    @Override
    public String toString() {
        return this.median + "/t" + this.stdDev;
    }
}
