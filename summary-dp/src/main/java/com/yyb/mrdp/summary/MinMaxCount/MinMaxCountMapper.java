package com.yyb.mrdp.summary.minmaxcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static com.yyb.mrdp.utils.MRDPUtils.transformXmlToMap;

/**
 * @author Administrator
 *         2016/6/19.
 */
public class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {

    // Our output key and value Writables
    private Text outUserId = new Text();
    private MinMaxCountTuple outTuple = new MinMaxCountTuple();

    // This object will format the creation date string into a Date object
    private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    static Logger logger = Logger.getLogger(MinMaxCountMapper.class);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            // Use function transformXmlToMap to transform the value input
            Map<String, String> parsed = transformXmlToMap(value.toString());

            // Grab the "CreationDate" field since it is what we are finding
            // the minDate and maxDate value of
            String strDate = parsed.get("CreationDate");

            // Grab the "UserId" field since it is what we are finding
            String userId = parsed.get("UserId");

            // Parse the string into a Date object
            Date creationDate = frmt.parse(strDate);

            // Set the minimum date and maximum date values to the creationDate
            outTuple.setMinDate(creationDate);
            outTuple.setMaxDate(creationDate);

            // Set the comment count to 1
            outTuple.setCount(1);

            // Set our user ID as the output key
            outUserId.set(userId);

            // Write out the hour and the average comment length
            context.write(outUserId, outTuple);

        } catch (Exception e) {
            // Print out error messages
            e.printStackTrace();
            logger.error(e);

        }

    }
}
