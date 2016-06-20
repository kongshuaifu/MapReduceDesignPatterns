package com.yyb.mrdp.summary.medianstddev.v2;

import com.yyb.mrdp.summary.medianstddev.MedianStdDevTuple;
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
 *         2016/6/20.
 */
public class MedianStdDevV2Tool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "MedianStdDevV2");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MedianStdDevTuple.class);

        job.setJarByClass(MedianStdDevV2Tool.class);
        job.setMapperClass(MedianStdDevV2Mapper.class);
        job.setCombinerClass(MedianStdDevV2Combiner.class);
        job.setReducerClass(MedianStdDevV2Reducer.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean result = job.waitForCompletion(true);

        return result ? 0 :1;
    }
}
