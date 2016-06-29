package com.yyb.mrdp.bloomfilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
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

import static com.yyb.mrdp.utils.MRDPUtils.transformXmlToMap;

/**
 * @author Administrator
 *         2016/6/28.
 */
public class HBBloomFilteringMapper extends Mapper<Object, Text, Text, NullWritable> {

    private BloomFilter filter = new BloomFilter();
    private HTable table = null;

    protected void setup(Context context) throws IOException, InterruptedException {

        // Get file from the Distributed Cache
        // URI[] files = DistributedCache.getCacheFiles(context .getConfiguration());
        URI[] files = context.getCacheFiles();
        System.out.println("Reading Bloom filter from: " + files[0].getPath());

        // Open local file for read.
        DataInputStream strm = new DataInputStream(new FileInputStream(files[0].getPath()));

        // Read into our Bloom filter.
        filter.readFields(strm);
        strm.close();

        // Get HBase table of user info
        Configuration hconf = HBaseConfiguration.create();
        table = new HTable(hconf, "user_table");
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String, String> parsed = transformXmlToMap(value.toString());

        // Get the value for the comment
        String userid = parsed.get("UserId");

        // If this user ID is in the set
        if (filter.membershipTest(new Key(userid.getBytes()))) {

            // Get the reputation from the HBase table
            Result r = table.get(new Get(userid.getBytes()));
            int reputation = Integer.parseInt(new String(r.getValue( "attr".getBytes(), "Reputation".getBytes())));

            // If the reputation is at least 1500,
            // write the record to the file system
            if (reputation >= 1500) {
                context.write(value, NullWritable.get());
            }
        }
    }





}
