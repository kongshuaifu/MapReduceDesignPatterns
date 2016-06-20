package com.yyb.mrdp.summary.medianstddev.v2;

import com.yyb.mrdp.summary.medianstddev.MedianStdDevTuple;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @author Administrator
 *         2016/6/20.
 */
public class MedianStdDevV2Reducer extends
        Reducer<IntWritable, SortedMapWritable,
                IntWritable, MedianStdDevTuple> {
    private MedianStdDevTuple result = new MedianStdDevTuple();
    private TreeMap<Integer, Long> commentLengthCounts = new TreeMap<>();

    public void reduce(IntWritable key, Iterable<SortedMapWritable> values,
                       Context context) throws IOException, InterruptedException {
        float sum = 0;
        long totalComments = 0;
        commentLengthCounts.clear();
        result.setMedian(0);
        result.setStdDev(0);
        for (SortedMapWritable v : values) {
            for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
                int length = ((IntWritable) entry.getKey()).get();
                long count = ((LongWritable) entry.getValue()).get();
                totalComments += count;
                sum += length * count;
                Long storedCount = commentLengthCounts.get(length);
                if (storedCount == null) {
                    commentLengthCounts.put(length, count);
                } else {
                    commentLengthCounts.put(length, storedCount + count);
                }
            }
        }
        long medianIndex = totalComments / 2L;
        long previousComments = 0;
        long comments = 0;
        int prevKey = 0;
        for (Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
            comments = previousComments + entry.getValue();
            if (previousComments <= medianIndex && medianIndex < comments) {
                if (totalComments % 2 == 0 && previousComments == medianIndex) {
                    result.setMedian((float) (entry.getKey() + prevKey) / 2.0f);
                } else {
                    result.setMedian(entry.getKey());
                }
                break;
            }
            previousComments = comments;
            prevKey = entry.getKey();
        }

        // calculate standard deviation
        float mean = sum / totalComments;
        float sumOfSquares = 0.0f;
        for (Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
            sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) *
                    entry.getValue();
        }
        result.setStdDev((float) Math.sqrt(sumOfSquares / (totalComments - 1)));
        context.write(key, result);
    }
}
