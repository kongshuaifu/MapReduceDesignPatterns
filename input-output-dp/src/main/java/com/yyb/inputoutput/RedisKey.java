package com.yyb.inputoutput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yyb on 16-7-12.
 */
public class RedisKey implements WritableComparable<PartitionPruningOutputDriver.RedisKey> {
    private int lastAccessMonth = 0;
    private Text field = new Text();

    public int getLastAccessMonth() {
        return this.lastAccessMonth;
    }

    public void setLastAccessMonth(int lastAccessMonth) {
        this.lastAccessMonth = lastAccessMonth;
    }

    public Text getField() {
        return this.field;
    }

    public void setField(String field) {
        this.field.set(field);
    }

    public void readFields(DataInput in) throws IOException {
        lastAccessMonth = in.readInt();
        this.field.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(lastAccessMonth);
        this.field.write(out);
    }

    public int compareTo(PartitionPruningOutputDriver.RedisKey rhs) {
        if (this.lastAccessMonth == rhs.getLastAccessMonth()) {
            return this.field.compareTo(rhs.getField());
        } else {
            return this.lastAccessMonth < rhs.getLastAccessMonth() ? -1 : 1;
        }
    }

    public String toString() {
        return this.lastAccessMonth + "\t" + this.field.toString();
    }

    public int hashCode() {
        return toString().hashCode();
    }
}
