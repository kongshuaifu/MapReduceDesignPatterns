package com.yyb.mrdp.structuredhierarchical;

import com.yyb.mrdp.utils.MRDPUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Map;


/**
 * @author Administrator
 *         2016/6/29.
 */
public class LastAccessDateBuildingDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LastAccessDateBuildingDriver");
        // Set custom partitioner and min last access date
        job.setPartitionerClass(LastAccessDatePartitioner.class);
        LastAccessDatePartitioner.setMinLastAccessDate(job, 2008);
        // Last access dates span between 2008-2011, or 4 years
        job.setNumReduceTasks(4);

    }

    public static class LastAccessDateMapper extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable outkey = new IntWritable();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
            // Grab the last access date
            String strDate = parsed.get("LastAccessDate");

            // Parse the string into a Calendar object
            LocalDate ld = LocalDate.parse(strDate);
            outkey.set(ld.getYear());

            // Write out the year with the input value
            context.write(outkey, value);
        }
    }


    public static class LastAccessDatePartitioner extends Partitioner<IntWritable, Text> implements Configurable {
        private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";
        private Configuration conf = null;
        private int minLastAccessDateYear = 0;

        public int getPartition(IntWritable key, Text value, int numPartitions) {
            return key.get() - minLastAccessDateYear;
        }

        public Configuration getConf() {
            return conf;
        }

        public void setConf(Configuration conf) {
            this.conf = conf;
            minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
        }

        public static void setMinLastAccessDate(Job job, int minLastAccessDateYear) {
            job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR, minLastAccessDateYear);
        }
    }


    public static class ValueReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                context.write(t, NullWritable.get());
            }
        }
    }
}
