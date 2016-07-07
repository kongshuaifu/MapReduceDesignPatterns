package com.yyb.mrdp.metapatterns;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by yyb on 16-7-7.
 */
public class ParallelJobs {

    public static class AverageReputationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private static final Text GROUP_ALL_KEY = new Text("Average Reputation:");
        private DoubleWritable outvalue = new DoubleWritable();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the line into tokens
            String[] tokens = value.toString().split("\t");
            // Get the reputation from the third column
            double reputation = Double.parseDouble(tokens[2]);
            // Set the output value and write to context
            outvalue.set(reputation);
            context.write(GROUP_ALL_KEY, outvalue);
        }
    }

    public static class AverageReputationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable outvalue = new DoubleWritable();

        protected void reduce(Text key, Iterable<DoubleWritable> values, Mapper.Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            double count = 0;
            for (DoubleWritable dw : values) {
                sum += dw.get();
                ++count;
            }
            outvalue.set(sum / count);
            context.write(key, outvalue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path belowAvgInputDir = new Path(args[0]);
        Path aboveAvgInputDir = new Path(args[1]);
        Path belowAvgOutputDir = new Path(args[2]);
        Path aboveAvgOutputDir = new Path(args[3]);
        Job belowAvgJob = submitJob(conf, belowAvgInputDir, belowAvgOutputDir);
        Job aboveAvgJob = submitJob(conf, aboveAvgInputDir, aboveAvgOutputDir);
        // While both jobs are not finished, sleep
        while (!belowAvgJob.isComplete() || !aboveAvgJob.isComplete()) {
            Thread.sleep(5000);
        }
        if (belowAvgJob.isSuccessful()) {
            System.out.println("Below average job completed successfully!");
        } else {
            System.out.println("Below average job failed!");
        }
        if (aboveAvgJob.isSuccessful()) {
            System.out.println("Above average job completed successfully!");
        } else {
            System.out.println("Above average job failed!");
        }
        System.exit(belowAvgJob.isSuccessful() && aboveAvgJob.isSuccessful() ? 0 : 1);
    }

    private static Job submitJob(Configuration conf, Path inputDir, Path outputDir) throws Exception {
        Job job = new Job(conf, "ParallelJobs");
        job.setJarByClass(ParallelJobs.class);
        job.setMapperClass(AverageReputationMapper.class);
        job.setReducerClass(AverageReputationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, inputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputDir);
        // Submit job and immediately return, rather than waiting for completion
        job.submit();
        return job;
    }
}
