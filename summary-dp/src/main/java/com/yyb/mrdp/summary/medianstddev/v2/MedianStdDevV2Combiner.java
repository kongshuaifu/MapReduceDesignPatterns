package com.yyb.mrdp.summary.medianstddev.v2;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map.Entry;

/**
 * @author Administrator
 *         2016/6/20.
 */
public class MedianStdDevV2Combiner extends Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable> {

    protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context)
            throws IOException, InterruptedException {
        SortedMapWritable outValue = new SortedMapWritable();
        for (SortedMapWritable v : values) {
            for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
                LongWritable count = (LongWritable) outValue.get(entry.getKey());
                if (count != null) {
                    count.set(count.get() + ((LongWritable) entry.getValue()).get());
                } else {
                    outValue.put(entry.getKey(), new LongWritable(
                            ((LongWritable) entry.getValue()).get()));
                }
            }
        }
        context.write(key, outValue);
    }
}
