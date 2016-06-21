package com.yyb.mrdp.summary.countcounters;

import com.yyb.mrdp.utils.MRDPUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.v2.app.job.Task;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

/**
 * @author Administrator
 *         2016/6/21.
 */
public class CountNumUserByStateMapper extends Mapper<Object, Text, NullWritable, NullWritable> {

    public static final String STATE_COUNTER_GROUP = "State";
    public static final String UNKNOWN_COUNTER = "Unknown";
    public static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";

    private String[] statesArray = new String[]{"AL", "AK", "AZ", "AR",
            "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
            "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
            "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
            "OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT",
            "VT", "VA", "WA", "WV", "WI", "WY"};

    private HashSet<String> states = new HashSet<>(Arrays.asList(statesArray));

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

        // Get the value for the Location attribute
        String location = parsed.get("Location");

        // Look for a state abbreviation code if the
        // location is not null or empty
        if (location != null && !location.isEmpty()) {

            // Make location uppercase and split on white space
            String[] tokens = location.toUpperCase().split("\\s");

            // For each token
            boolean unknown = true;
            for (String state : tokens) {
                // Check if it is a state
                if (states.contains(state)) {
                    // If so, increment the state's counter by 1
                    // and flag it as not unknown
                    context.getCounter(STATE_COUNTER_GROUP, state).increment(1);
                    unknown = false;
                    break;
                }
            }

            // If the state is unknown, increment the UNKNOWN_COUNTER counter
            if (unknown) {
                context.getCounter(STATE_COUNTER_GROUP, UNKNOWN_COUNTER) .increment(1);
            }
        } else {
            // If it is empty or null, increment the
            // NULL_OR_EMPTY_COUNTER counter by 1
            context.getCounter(STATE_COUNTER_GROUP, NULL_OR_EMPTY_COUNTER).increment(1);
        }
    }


}
