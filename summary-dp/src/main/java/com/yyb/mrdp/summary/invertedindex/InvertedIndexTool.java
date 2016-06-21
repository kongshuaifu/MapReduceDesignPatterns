package com.yyb.mrdp.summary.invertedindex;

import com.yyb.mrdp.summary.average.AverageTuple;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @author Administrator
 *         2016/6/21.
 */
public class InvertedIndexTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "InvertedIndexTool");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(WikipediaExtractor.class);
        job.setCombinerClass(Concatenator.class);
        job.setReducerClass(Concatenator.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
