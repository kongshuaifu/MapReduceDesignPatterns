package com.yyb.mrdp.summary.invertedindex;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

import static com.yyb.mrdp.utils.MRDPUtils.transformXmlToMap;

/**
 * @author Administrator
 *         2016/6/21.
 */
public class WikipediaExtractor extends Mapper<Object, Text, Text, Text>{

    private Text link = new Text();
    private Text outkey = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String ,String> parsed = transformXmlToMap(value.toString());

        // Grab the necessary XML attributes
        String txt = parsed.get("Body");
        String posttype = parsed.get("PostTypeId");
        String row_id = parsed.get("Id");

        // if the body is null, or the post is a question (1), skip
        if (txt == null || (posttype != null && posttype.equals("1"))) {
            return;
        }
        // Unescape the HTML because the SO data is escaped.
        txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
        link.set(getWikipediaURL(txt));
        outkey.set(row_id);
        context.write(link, outkey);
    }


    private String getWikipediaURL(String txt){
        String url = null;
        // do something

        return url;
    }
}
