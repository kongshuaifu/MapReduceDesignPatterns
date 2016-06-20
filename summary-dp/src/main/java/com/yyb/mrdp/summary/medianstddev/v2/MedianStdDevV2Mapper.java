package com.yyb.mrdp.summary.medianstddev.v2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

import static com.yyb.mrdp.utils.MRDPUtils.transformXmlToMap;

/**
 * @author Administrator
 *         2016/6/20.
 *         中位数 与 标准差
 */
public class MedianStdDevV2Mapper extends Mapper<Object, Text, IntWritable, SortedMapWritable> {

    private IntWritable commentLength = new IntWritable();
    private static final LongWritable ONE = new LongWritable(1);
    private IntWritable outHour = new IntWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        Map<String, String> parsed = transformXmlToMap(value.toString());
        // Grab the "CreationDate" field,
        // since it is what we are grouping by
        String strDate = parsed.get("CreationDate");

        // Grab the comment to find the length
        String text = parsed.get("Text");
        // Get the hour this comment was posted in

        LocalDateTime creationDate = LocalDateTime.parse(strDate);
        outHour.set(creationDate.getHour());
        commentLength.set(text.length());

        SortedMapWritable outCommentLength = new SortedMapWritable();
        outCommentLength.put(commentLength, ONE);

        // Write out the user ID with min max dates and count
        context.write(outHour, outCommentLength);
    }


}
