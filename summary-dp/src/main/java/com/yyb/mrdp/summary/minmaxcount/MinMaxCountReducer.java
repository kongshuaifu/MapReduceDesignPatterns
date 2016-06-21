package com.yyb.mrdp.summary.minmaxcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Administrator
 *         2016/6/19.
 */
public class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

    // Our output value Writable
    private MinMaxCountTuple result = new MinMaxCountTuple();

    @Override
    protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {

        // Initialize our result
        result.setMinDate(null);
        result.setMaxDate(null);
        result.setCount(0);
        int sum = 0;

        // Iterate through all input values for this key
        for (MinMaxCountTuple val : values) {

            // If the value's minDate is less than the result's minDate
            // Set the result's minDate to value's
            if (result.getMinDate() == null ||
                    val.getMinDate().compareTo(result.getMinDate()) < 0) {
                result.setMinDate(val.getMinDate());
            }

            // If the value's maxDate is more than the result's maxDate
            // Set the result's maxDate to value's
            if (result.getMaxDate() == null ||
                    val.getMaxDate().compareTo(result.getMaxDate()) < 0) {
                result.setMaxDate(val.getMaxDate());
            }

            // Add to our sum the count for value
            sum += val.getCount();

        }

        // Set our count to the number of input values
        result.setCount(sum);

        context.write(key, result);

    }
}
