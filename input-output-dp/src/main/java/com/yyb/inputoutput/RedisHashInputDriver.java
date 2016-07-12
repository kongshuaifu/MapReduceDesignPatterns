package com.yyb.inputoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import static com.yyb.mrdp.utils.MRDPUtils.transformXmlToMap;

/**
 * Created by yyb on 16-7-12.
 */
public class RedisHashInputDriver {

    public static class RedisHashInputFormat extends InputFormat<Text, Text> {
        public static final String REDIS_HOSTS_CONF =
                "mapred.redishashinputformat.hosts";
        public static final String REDIS_HASH_KEY_CONF =
                "mapred.redishashinputformat.key";
        private static final Logger LOG = Logger
                .getLogger(RedisHashInputFormat.class);

        public static void setRedisHosts(Job job, String hosts) {
            job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
        }

        public static void setRedisHashKey(Job job, String hashKey) {
            job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
        }

        public List<InputSplit> getSplits(JobContext job) throws IOException {
            String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
            if (hosts == null || hosts.isEmpty()) {
                throw new IOException(REDIS_HOSTS_CONF
                                              + " is not set in configuration.");
            }
            String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
            if (hashKey == null || hashKey.isEmpty()) {
                throw new IOException(REDIS_HASH_KEY_CONF
                                              + " is not set in configuration.");
            }
// Create an input split for each host
            List<InputSplit> splits = new ArrayList<InputSplit>();
            for (String host : hosts.split(",")) {
                splits.add(new RedisHashInputSplit(host, hashKey));
            }
            LOG.info("Input splits to process: " + splits.size());
            return splits;
        }

        public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new RedisHashRecordReader();
        }

        public static class RedisHashRecordReader extends RecordReader<Text, Text> {
            private static final Logger LOG =
                    Logger.getLogger(RedisHashRecordReader.class);
            private Iterator<Map.Entry<String, String>> keyValueMapIter = null;
            private Text key = new Text(), value = new Text();
            private float processedKVs = 0, totalKVs = 0;
            private Map.Entry<String, String> currentEntry = null;

            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                // Get the host location from the InputSplit
                String host = split.getLocations()[0];
                String hashKey = ((RedisHashInputSplit) split).getHashKey();
                LOG.info("Connecting to " + host + " and reading from "
                                 + hashKey);
                Jedis jedis = new Jedis(host);
                jedis.connect();
                jedis.getClient().setTimeoutInfinite();
                // Get all the key/value pairs from the Redis instance and store
                // them in memory
                totalKVs = jedis.hlen(hashKey);
                keyValueMapIter = jedis.hgetAll(hashKey).entrySet().iterator();
                LOG.info("Got " + totalKVs + " from " + hashKey);
                jedis.disconnect();
            }

            public boolean nextKeyValue() throws IOException,
                    InterruptedException {
                // If the key/value map still has values
                if (keyValueMapIter.hasNext()) {
                    // Get the current entry and set the Text objects to the entry
                    currentEntry = keyValueMapIter.next();
                    key.set(currentEntry.getKey());
                    value.set(currentEntry.getValue());
                    return true;
                } else {
                    // No more values? return false.
                    return false;
                }
            }

            public Text getCurrentKey() throws IOException,
                    InterruptedException {
                return key;
            }

            public Text getCurrentValue() throws IOException,
                    InterruptedException {
                return value;
            }

            public float getProgress() throws IOException, InterruptedException {
                return processedKVs / totalKVs;
            }

            public void close() throws IOException {
                // nothing to do here
            }
        }

        public static class RedisHashInputSplit extends InputSplit implements Writable {
            private String location = null;
            private String hashKey = null;

            public RedisHashInputSplit() {
                // Default constructor for reflection
            }

            public RedisHashInputSplit(String redisHost, String hash) {
                this.location = redisHost;
                this.hashKey = hash;
            }

            public String getHashKey() {
                return this.hashKey;
            }

            public void readFields(DataInput in) throws IOException {
                this.location = in.readUTF();
                this.hashKey = in.readUTF();
            }

            public void write(DataOutput out) throws IOException {
                out.writeUTF(location);
                out.writeUTF(hashKey);
            }

            public long getLength() throws IOException, InterruptedException {
                return 0;
            }

            public String[] getLocations() throws IOException, InterruptedException {
                return new String[]{location};
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
        String hosts = args[0];
        String hashKey = args[1];
        Path outputDir = new Path(args[2]);
        Job job = new Job(conf, "Redis Input");
        job.setJarByClass(RedisHashInputDriver.class);
        // Use the identity mapper
        job.setNumReduceTasks(0);
        job.setInputFormatClass(RedisHashInputFormat.class);
        RedisHashInputFormat.setRedisHosts(job, hosts);
        RedisHashInputFormat.setRedisHashKey(job, hashKey);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputDir);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 3);
    }
}
