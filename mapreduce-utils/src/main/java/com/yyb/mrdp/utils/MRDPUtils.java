package com.yyb.mrdp.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Administrator
 *         2016/6/19.
 */
public class MRDPUtils {

    /**
     * This helper function parses the stackoverflow into a Map for us.
     *
     * @param xml data for xml
     * @return Map
     */
    public static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<>();
        try {
            xml = xml.trim();
            String[] tokens = xml.substring(5, xml.length() - 3).split("\"");

            for (int i = 0; i < tokens.length; i += 2) {
                String key = tokens[i].trim();
                String value = tokens[i + 1];

                // 去除 = 号
                map.put(key.substring(0, key.length() - 1), value);
            }

        } catch (Exception e) {
            System.err.println(xml);
            e.printStackTrace();
        }


        return map;
    }

    public static void printOptions(List<String> classList) {
        System.out.println("");
        System.err.println("hadoop jar **.jar <parameters...> classname inputpath outputpath");
        System.err.println("parameters like : -Dmapreduce.job.reduces=2");
        System.err.println("classname under that ");
        classList.forEach(name ->
            System.err.println("\t\t" + name)
        );
        System.err.println("inputpath : the path where the data will be read");
        System.err.println("inputpath : the path where the result write, it must be null and it would auto create");
        System.out.println("");
    }

}
