package com.yyb.mrdp.summary.minmaxcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @author Administrator
 *         2016/6/19.
 */
public class MinMaxCountTuple implements Writable {

    private Date minDate = new Date();
    private Date maxDate = new Date();
    private long count = 0;
    private SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.getDefault());

    public Date getMinDate() {
        return minDate;
    }

    public void setMinDate(Date minDate) {
        this.minDate = minDate;
    }

    public Date getMaxDate() {
        return maxDate;
    }

    public void setMaxDate(Date maxDate) {
        this.maxDate = maxDate;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        // Write the data out in the order it is read,
        // using the UNIX timestamp to represent the Date
        out.writeLong(minDate.getTime());
        out.writeLong(maxDate.getTime());
        out.writeLong(count);
    }

    public void readFields(DataInput in) throws IOException {
        // Read the data out in the order it is written,
        // creating new Date objects from the UNIX timestamp
        minDate.setTime(in.readLong());
        maxDate.setTime(in.readLong());
        count = in.readLong();
    }


    @Override
    public String toString() {
        return frmt.format(minDate) + "\t" + frmt.format(minDate) + "\t" + count;
    }
}
