package com.yyb.inputoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.yyb.mrdp.utils.MRDPUtils.transformXmlToMap;

/**
 * Created by yyb on 16-7-12.
 */
public class RedisHashOutputDriver {

    public static class RedisHashOutputFormat extends OutputFormat<Text, Text> {
        public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts";
        public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";

        public static void setRedisHosts(Job job, String hosts) {
            job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
        }

        public static void setRedisHashKey(Job job, String hashKey) {
            job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
        }

        public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {

            return new RedisHashRecordWriter(job.getConfiguration().get(
                    REDIS_HASH_KEY_CONF), job.getConfiguration().get(
                    REDIS_HOSTS_CONF));
        }

        public void checkOutputSpecs(JobContext job) throws IOException {
            String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
            if (hosts == null || hosts.isEmpty()) {
                throw new IOException(REDIS_HOSTS_CONF
                                              + " is not set in configuration.");
            }
            String hashKey = job.getConfiguration().get(
                    REDIS_HASH_KEY_CONF);
            if (hashKey == null || hashKey.isEmpty()) {
                throw new IOException(REDIS_HASH_KEY_CONF
                                              + " is not set in configuration.");
            }
        }

        public OutputCommitter getOutputCommitter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
        }

        public static class RedisHashRecordWriter extends RecordWriter<Text, Text> {
            private HashMap<Integer, Jedis> jedisMap = new HashMap<Integer, Jedis>();
            private String hashKey = null;

            public RedisHashRecordWriter(String hashKey, String hosts) {
                this.hashKey = hashKey;
                // Create a connection to Redis for each host
                // Map an integer 0-(numRedisInstances - 1) to the instance
                int i = 0;
                for (String host : hosts.split(",")) {
                    Jedis jedis = new Jedis(host);
                    jedis.connect();
                    jedisMap.put(i, jedis);
                    ++i;
                }
            }

            public void write(Text key, Text value) throws IOException,
                    InterruptedException {
                // Get the Jedis instance that this key/value pair will be
                // written to
                Jedis j = jedisMap.get(Math.abs(key.hashCode()) % jedisMap.size());
                // Write the key/value pair
                j.hset(hashKey, key.toString(), value.toString());
            }

            public void close(TaskAttemptContext context) throws IOException,
                    InterruptedException {
                for (Jedis jedis : jedisMap.values()) {
                    jedis.disconnect();
                }
            }
        }

    }

    public static class RedisOutputMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Map<String, String> parsed = transformXmlToMap(value.toString());
            String userId = parsed.get("Id");
            String reputation = parsed.get("Reputation");
            // Set our output key and values
            outkey.set(userId);
            outvalue.set(reputation);
            context.write(outkey, outvalue);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        String hosts = args[1];
        String hashName = args[2];
        Job job = Job.getInstance(conf, "Redis Output");
        job.setJarByClass(RedisHashOutputDriver.class);
        job.setMapperClass(RedisOutputMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, inputPath);
        job.setOutputFormatClass(RedisHashOutputFormat.class);
        RedisHashOutputFormat.setRedisHosts(job, hosts);
        RedisHashOutputFormat.setRedisHashKey(job, hashName);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        int code = job.waitForCompletion(true) ? 0 : 2;
        System.exit(code);
    }
}
