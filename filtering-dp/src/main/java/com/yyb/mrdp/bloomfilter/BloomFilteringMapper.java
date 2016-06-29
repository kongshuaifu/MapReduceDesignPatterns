package com.yyb.mrdp.bloomfilter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;

import static com.yyb.mrdp.utils.MRDPUtils.transformXmlToMap;

/**
 * @author Administrator
 *         2016/6/28.
 */
public class BloomFilteringMapper extends Mapper<Object, Text, Text, NullWritable> {

    private BloomFilter filter = new BloomFilter();

    protected void setup(Context context) throws IOException,InterruptedException {

        // Get file from the DistributedCache
        // URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
        URI[] files = context.getCacheFiles();
        System.out.println("Reading Bloom filter from: " + files[0].getPath());
        // Open local file for read.
        DataInputStream strm = new DataInputStream(new FileInputStream(files[0].getPath()));
        // Read into our Bloom filter.
        filter.readFields(strm);
        strm.close();
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String, String> parsed = transformXmlToMap(value.toString());

        // Get the value for the comment
        String comment = parsed.get("Text");
        StringTokenizer tokenizer = new StringTokenizer(comment);

        // For each word in the comment
        while (tokenizer.hasMoreTokens()) {

            // If the word is in the filter, output the record and break
            String word = tokenizer.nextToken();
            if (filter.membershipTest(new Key(word.getBytes()))) {
                context.write(value, NullWritable.get());
                break;
            }
        }
    }
}