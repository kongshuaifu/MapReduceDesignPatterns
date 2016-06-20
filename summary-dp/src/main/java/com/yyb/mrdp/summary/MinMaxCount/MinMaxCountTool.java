package com.yyb.mrdp.summary.minmaxcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @author Administrator
 *         2016/6/19.
 */
public class MinMaxCountTool extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "minmaxcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxCountTuple.class);

        job.setJarByClass(MinMaxCountTool.class);
        job.setMapperClass(MinMaxCountMapper.class);
        job.setCombinerClass(MinMaxCountReducer.class);
        job.setReducerClass(MinMaxCountReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean result = job.waitForCompletion(true);

        return result ? 0 :1;
    }
}
