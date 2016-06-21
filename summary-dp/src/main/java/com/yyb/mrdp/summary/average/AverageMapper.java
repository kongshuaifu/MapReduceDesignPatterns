package com.yyb.mrdp.summary.average;

import com.yyb.mrdp.utils.MRDPUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * @author Administrator
 *         2016/6/19.
 */
public class AverageMapper extends Mapper<Object, Text, IntWritable, AverageTuple> {

    private IntWritable outHour = new IntWritable();
    private AverageTuple outAverage = new AverageTuple();
    static Logger logger = Logger.getLogger(AverageMapper.class);


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String ,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
        try {

            // Grab the "CreationDate" field,
            // since it is what we are grouping by
            String strDate = parsed.get("CreationDate");

            // Grab the comment to find the length
            String text = parsed.get("Text");

            // get the hour this comment was posted in
            LocalDateTime creationDate = LocalDateTime.parse(strDate);
            outHour.set(creationDate.getHour());

            // get the comment length
            outAverage.setCount(1);
            outAverage.setAverage(text.length());

            // write out the hour with the comment length
            context.write(outHour, outAverage);
        }catch (Exception e){
            e.printStackTrace();
            logger.error(e);
        }


    }
}
