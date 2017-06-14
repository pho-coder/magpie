package com.jd.bdp.magpie.util;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-12-13
 * Time: 下午4:23
 * To change this template use File | Settings | File Templates.
 */
public class Utils {

    public static String getPid() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String name = runtime.getName(); // format: "pid@hostname"
        return name.substring(0, name.indexOf('@'));
    }

    public static String bytesToString(byte[] bytes) throws UnsupportedEncodingException {
        return new String(bytes, "utf-8");
    }

    public static byte[] stringToBytes(String string) throws UnsupportedEncodingException {
        return string.getBytes("utf-8");
    }

    public static HashMap<String, Object> stringToMap(String jsonString) {
        JSONObject jsonObject = new JSONObject(jsonString);
        Iterator<String> it = jsonObject.keys();
        HashMap<String, Object> result = new HashMap<String, Object>();
        while (it.hasNext()) {
            String key = it.next();
            result.put(key, jsonObject.get(key));
        }
        return result;
    }

    public static HashMap<String, Object> bytesToMap(byte[] bytes) throws UnsupportedEncodingException {
        return stringToMap(bytesToString(bytes));
    }

}
