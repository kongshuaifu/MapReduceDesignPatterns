package com.yyb.mrdp.summary.countcounters;

import com.yyb.mrdp.summary.invertedindex.Concatenator;
import com.yyb.mrdp.summary.invertedindex.WikipediaExtractor;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @author Administrator
 *         2016/6/21.
 */
public class CountNumUserByStateTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "CountNumUserByStateTool");

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(CountNumUserByStateMapper.class);
//        job.setCombinerClass(Concatenator.class);
//        job.setReducerClass(Concatenator.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        int code = job.waitForCompletion(true) ? 0 : 1;

        if(code == 0){
            for(Counter counter : job.getCounters().getGroup(CountNumUserByStateMapper.STATE_COUNTER_GROUP)){
                System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
            }
        }

        return code;
    }
}
