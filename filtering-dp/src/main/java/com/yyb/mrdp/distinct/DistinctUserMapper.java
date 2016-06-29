package com.yyb.mrdp.distinct;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

import static com.yyb.mrdp.utils.MRDPUtils.transformXmlToMap;

/**
 * @author Administrator
 *         2016/6/28.
 */
public class DistinctUserMapper extends Mapper<Object, Text, Text, NullWritable> {

    private Text outUserId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String, String> parsed = transformXmlToMap(value.toString());

        // Get the value for the UserId attribute
        String userId = parsed.get("UserId");

        // Set our output key to the user's id
        outUserId.set(userId);

        // Write the user's id with a null value
        context.write(outUserId, NullWritable.get());
    }
}
