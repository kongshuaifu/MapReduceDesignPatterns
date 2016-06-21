package com.yyb.mrdp.summary.average;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Administrator
 *         2016/6/19.
 */
public class AverageReducer extends Reducer<IntWritable, AverageTuple, IntWritable, AverageTuple> {

    private AverageTuple result = new AverageTuple();

    @Override
    protected void reduce(IntWritable key, Iterable<AverageTuple> values, Context context) throws IOException, InterruptedException {
        result.setCount(0);
        result.setAverage(0);

        int count = 0;
        float sum = 0;

        // Iterate through all input values for this key
        for (AverageTuple val : values) {
            count += val.getCount();
            sum += val.getCount() * val.getAverage();
        }

        result.setCount(count);
        result.setAverage(sum / count);
        context.write(key, result);

    }
}
