package com.wildgoose.gmall.realtime.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author 张翔
 * @date 2023/2/23 17:25
 * @description
 */
public class PropertiesUtil {
    //    private static final Log logger = LogFactory.getLog(PropertiesUtil.class);
    private static Properties prop = new Properties();
    private static final String name = "/application.properties";

    static {
        InputStream in = PropertiesUtil.class.getResourceAsStream(name);
        try {
            prop.load(in);
        } catch (IOException e) {
            LoggerUtil.log().error(e);
        }
    }

    public static String get(String key) {
        if (prop.containsKey(key)) {
            return prop.getProperty(key);
        } else {
            LoggerUtil.log().error("Key不存在：" + key);
            return null;
        }
    }

    public static Long getLong(String key) {

        return Long.parseLong(prop.getProperty(key));
    }

    public static Integer getInt(String key) {

        return Integer.parseInt(prop.getProperty(key));
    }
}
