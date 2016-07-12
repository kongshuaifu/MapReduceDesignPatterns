package com.yyb.inputoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by yyb on 16-7-12.
 */
public class PartitionPruningInputDriver {

    public static class RedisLastAccessInputSplit extends InputSplit implements Writable {
        private String location = null;
        private List<String> hashKeys = new ArrayList<String>();

        public RedisLastAccessInputSplit() {
            // Default constructor for reflection
        }

        public RedisLastAccessInputSplit(String redisHost) {
            this.location = redisHost;
        }

        public void addHashKey(String key) {
            hashKeys.add(key);
        }

        public void removeHashKey(String key) {
            hashKeys.remove(key);
        }

        public List<String> getHashKeys() {
            return hashKeys;
        }

        public void readFields(DataInput in) throws IOException {
            location = in.readUTF();
            int numKeys = in.readInt();

            hashKeys.clear();
            for (int i = 0; i < numKeys; ++i) {
                hashKeys.add(in.readUTF());
            }
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(location);
            out.writeInt(hashKeys.size());
            for (String key : hashKeys) {
                out.writeUTF(key);
            }
        }

        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        public String[] getLocations() throws IOException, InterruptedException {
            return new String[]{location};
        }
    }

    public static class RedisLastAccessInputFormat  extends InputFormat<RedisKey, Text> {
        public static final String REDIS_SELECTED_MONTHS_CONF =
                "mapred.redilastaccessinputformat.months";
        private static final HashMap<String, Integer> MONTH_FROM_STRING =
                new HashMap<String, Integer>();
        private static final HashMap<String, String> MONTH_TO_INST_MAP =
                new HashMap<String, String>();
        private static final Logger LOG = Logger.getLogger(RedisLastAccessInputFormat.class);

        static {
            // Initialize month to Redis instance map
            // Initialize month 3 character code to integer
        }

        public static void setRedisLastAccessMonths(Job job, String months) {
            job.getConfiguration().set(REDIS_SELECTED_MONTHS_CONF, months);
        }

        public List<InputSplit> getSplits(JobContext job) throws IOException {
            String months = job.getConfiguration().get(
                    REDIS_SELECTED_MONTHS_CONF);
            if (months == null || months.isEmpty()) {
                throw new IOException(REDIS_SELECTED_MONTHS_CONF
                                              + " is null or empty.");
            }
            // Create input splits from the input months
            HashMap<String, RedisLastAccessInputSplit> instanceToSplitMap =
                    new HashMap<String, RedisLastAccessInputSplit>();
            for (String month : months.split(",")) {
                String host = MONTH_TO_INST_MAP.get(month);
                RedisLastAccessInputSplit split = instanceToSplitMap.get(host);
                if (split == null) {
                    split = new RedisLastAccessInputSplit(host);
                    split.addHashKey(month);
                    instanceToSplitMap.put(host, split);
                } else {
                    split.addHashKey(month);
                }
            }
            LOG.info("Input splits to process: " +
                             instanceToSplitMap.values().size());
            return new ArrayList<InputSplit>(instanceToSplitMap.values());
        }

        public RecordReader<RedisKey, Text> createRecordReader(
                InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new RedisLastAccessRecordReader();
        }

        public static class RedisLastAccessRecordReader extends RecordReader<RedisKey, Text> {
            private static final Logger LOG = Logger.getLogger(RedisLastAccessRecordReader.class);
            private Map.Entry<String, String> currentEntry = null;
            private float processedKVs = 0, totalKVs = 0;
            private int currentHashMonth = 0;
            private Iterator<Map.Entry<String, String>> hashIterator = null;
            private Iterator<String> hashKeys = null;
            private RedisKey key = new RedisKey();
            private String host = null;
            private Text value = new Text();

            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                // Get the host location from the InputSplit
                host = split.getLocations()[0];
                // Get an iterator of all the hash keys we want to read
                hashKeys = ((RedisLastAccessInputSplit) split)
                        .getHashKeys().iterator();
                LOG.info("Connecting to " + host);
            }

            @Override
            public RedisKey getCurrentKey() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public Text getCurrentValue() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return 0;
            }

            @Override
            public void close() throws IOException {

            }

            public boolean nextKeyValue() throws IOException,
                    InterruptedException {
                boolean nextHashKey = false;
                do {
                    // if this is the first call or the iterator does not have a
                    // next
                    if (hashIterator == null || !hashIterator.hasNext()) {
                        // if we have reached the end of our hash keys, return
                        // false
                        if (!hashKeys.hasNext()) {
                            // ultimate end condition, return false
                            return false;
                        } else {
                            // Otherwise, connect to Redis and get all
                            // the name/value pairs for this hash key
                            Jedis jedis = new Jedis(host);
                            jedis.connect();
                            String strKey = hashKeys.next();
                            currentHashMonth = MONTH_FROM_STRING.get(strKey);
                            hashIterator = jedis.hgetAll(strKey).entrySet()
                                                .iterator();
                            jedis.disconnect();
                        }
                    }
                    // If the key/value map still has values
                    if (hashIterator.hasNext()) {
                        // Get the current entry and set
                        // the Text objects to the entry
                        currentEntry = hashIterator.next();
                        key.setLastAccessMonth(currentHashMonth);
                        key.setField(currentEntry.getKey());
                        value.set(currentEntry.getValue());
                    } else {
                        nextHashKey = true;
                    }
                } while (nextHashKey);
                return true;
            }

        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String lastAccessMonths = args[0];
        Path outputDir = new Path(args[1]);
        Job job = new Job(conf, "Redis Input");
        job.setJarByClass(PartitionPruningInputDriver.class);
        // Use the identity mapper
        job.setNumReduceTasks(0);
        job.setInputFormatClass(RedisLastAccessInputFormat.class);
        RedisLastAccessInputFormat.setRedisLastAccessMonths(job, lastAccessMonths);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputDir);
        job.setOutputKeyClass(RedisKey.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}
