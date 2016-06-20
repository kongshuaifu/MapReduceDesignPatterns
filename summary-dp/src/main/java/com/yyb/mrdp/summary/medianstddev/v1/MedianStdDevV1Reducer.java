package com.yyb.mrdp.summary.medianstddev.v1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * @author Administrator
 *         2016/6/20.
 */
public class MedianStdDevV1Reducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevV1Tuple> {

    private MedianStdDevV1Tuple result = new MedianStdDevV1Tuple();
    private ArrayList<Float> commentLengths = new ArrayList<>();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        float sum = 0;
        float count = 0;
        commentLengths.clear();
        result.setStdDev(0);

        // Iterate through all input values for this key
        for (IntWritable val : values) {
            commentLengths.add((float) val.get());
            sum += val.get();
            ++count;
        }

        // sort commentLengths to calculate median
        Collections.sort(commentLengths);

        // if commentLengths is an even value, average middle two elements
        if (count % 2 == 0) {
            result.setMedian((commentLengths.get((int) count / 2 - 1) +
                    commentLengths.get((int) count / 2)) / 2.0f);
        } else {
            // else, set median to middle value
            result.setMedian(commentLengths.get((int) count / 2));
        }

        // calculate standard deviation
        float mean = sum / count;
        float sumOfSquares = 0.0f;
        for (Float f : commentLengths) {
            sumOfSquares += (f - mean) * (f - mean);
        }

        result.setStdDev((float) Math.sqrt(sumOfSquares / (count - 1)));

        context.write(key, result);
    }
}
