package com.yyb.mrdp.filtering;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Administrator
 *         2016/6/26.
 * @note As this is a map-only job, there is no combiner or reducer. All output records will be
 * written directly to the file system.
 */
public class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {

    private String mapRegex = null;

    public void setup(Context context) throws IOException, InterruptedException {
        mapRegex = context.getConfiguration().get("mapregex");
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().matches(mapRegex)) {
            context.write(NullWritable.get(), value);
        }
    }
}
