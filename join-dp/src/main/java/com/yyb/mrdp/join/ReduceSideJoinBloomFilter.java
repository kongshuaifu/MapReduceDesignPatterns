package com.yyb.mrdp.join;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import static com.yyb.mrdp.utils.MRDPUtils.transformXmlToMap;

/**
 * Created by yyb on 16-7-7.
 */
public class ReduceSideJoinBloomFilter {

    public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = transformXmlToMap(value.toString());

            // If the reputation is greater than 1,500,
            // output the user ID with the value
            if (Integer.parseInt(parsed.get("Reputation")) > 1500) {
                outKey.set(parsed.get("Id"));
                outValue.set("A" + value.toString());
                context.write(outKey, outValue);
            }
        }
    }

    public static class CommentJoinMapperWithBloom extends Mapper<Object, Text, Text, Text> {
        private BloomFilter bfilter = new BloomFilter();
        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void setup(Context context) throws IOException {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            DataInputStream strm = new DataInputStream(
                    new FileInputStream(new File(files[0].toString())));
            bfilter.readFields(strm);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = transformXmlToMap(value.toString());
            String userId = parsed.get("UserId");
            if (bfilter.membershipTest(new Key(userId.getBytes()))) {
                outkey.set(userId);
                outvalue.set("B" + value.toString());
                context.write(outkey, outvalue);
            }
        }
    }


}
