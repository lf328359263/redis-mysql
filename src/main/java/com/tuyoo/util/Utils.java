package com.tuyoo.util;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 工具类
 */
public class Utils {

    public static Logger LOG = Logger.getLogger(Utils.class);

    public static List<String> parseToList(String key) {
        String[] keys = key.split(":|_");
        List<String> result = new ArrayList<String>(Arrays.asList(keys));
        result.remove(1);
        return result;
    }

    public static Properties getProperties(String path){
        Properties properties = new Properties();
        try {
            FileInputStream file = new FileInputStream(path);
            properties.load(file);
            LOG.info("文件正常加载："+path);
        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return properties;
    }

}
