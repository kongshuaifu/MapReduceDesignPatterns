package com.yyb.mrdp.summary.invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Administrator
 *         2016/6/21.
 */
public class Concatenator extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Text id : values){

            if (first){
                first = false;
            }else{
                sb.append(" ");
            }
            sb.append(id.toString());
        }

        result.set(sb.toString());

        context.write(key, result);
    }
}
