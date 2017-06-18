package com.haole.mq.beanstalk;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shengjunzhao on 2017/5/29.
 */
public class YamlUtil {

    public static List<String> yaml2List(Charset charset, byte[] data) throws IOException {
        List<String> list = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data), charset));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] kvs = line.split(" ");
            if (kvs.length == 2) {
                list.add(kvs[1].trim());
            }
        }
        return list;
    }

    public static Map<String, String> yaml2Map(Charset charset, byte[] data) throws IOException {
        Map<String, String> map = new HashMap<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data), charset));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] kvs = line.split(":");
            if (kvs.length == 2) {
                map.put(kvs[0].trim(), kvs[1].trim());
            }
        }
        return map;
    }
}
