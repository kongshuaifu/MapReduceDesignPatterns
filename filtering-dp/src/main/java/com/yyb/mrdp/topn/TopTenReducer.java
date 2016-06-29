package com.yyb.mrdp.topn;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static com.yyb.mrdp.utils.MRDPUtils.transformXmlToMap;

/**
 * @author Administrator
 *         2016/6/28.
 */
public class TopTenReducer extends
        Reducer<NullWritable, Text, NullWritable, Text> {
    // Stores a map of user reputation to the record
// Overloads the comparator to order the reputations in descending order
    private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
    public void reduce(NullWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            Map<String, String> parsed = transformXmlToMap(value.toString());
            repToRecordMap.put(Integer.parseInt(parsed.get("Reputation")),
                    new Text(value));
// If we have more than ten records, remove the one with the lowest rep
// As this tree map is sorted in descending order, the user with
// the lowest reputation is the last key.
            if (repToRecordMap.size() > 10) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }
        for (Text t : repToRecordMap.descendingMap().values()) {
// Output our ten records to the file system with a null key
            context.write(NullWritable.get(), t);
        }
    }
}
