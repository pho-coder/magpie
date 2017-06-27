package com.jd.bdp.magpie.utils;

import org.json.JSONObject;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-12-13
 * Time: 下午3:37
 * To change this template use File | Settings | File Templates.
 */
public class Utils {

    public static Map readConfig(String jrdwConfig) {
        Map props = null;
        try {
            props = findAndReadConfigFile("magpie.yaml", true);
            if (jrdwConfig != null) {
                props.putAll(findAndReadConfigFile(jrdwConfig, false));
            }
            if (props.get(Constants.ZKSERVERS) == null) {
                throw new IOException("ZKSERVERS can not be null");
            }
            if (!props.containsKey(Constants.ZKROOT)) {
                props.put(Constants.ZKROOT, "/magpie");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return props;
    }

    public static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL> ret = new ArrayList<URL>();
            while (resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map findAndReadConfigFile(String name, boolean mustExist) {
        try {
            List<URL> resources = findResources(name);
            if (resources.isEmpty()) {
                if (mustExist) throw new RuntimeException("Could not find config file on classpath " + name);
                else return new HashMap();
            }
            if (resources.size() > 1) {
                throw new RuntimeException("Found multiple " + name + " resources."
                        + resources);
            }
            URL resource = resources.get(0);
            Yaml yaml = new Yaml();
            Map ret = (Map) yaml.load(new InputStreamReader(resource.openStream()));
            if (ret == null) ret = new HashMap();
            return new HashMap(ret);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map findAndReadConfigFile(String name) {
        return findAndReadConfigFile(name, false);
    }

    public static String getNimbusPath() {
        return "/nimbus";
    }

    public static String getSupervisorsPath() {return "/supervisors";}

    public static String getStatusPath() {
        return "/status";
    }

    public static String getResourceWebServiceZkPath() {
        return "/webservice/resource";
    }

    public static String getAssignmentsPath() { return "/assignments"; }


    public static String getStatusNode(String id) {
        return getStatusPath() + "/" + id;
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

    public static int str2int(String str) {
        if (str == null || str.equals("null")) {
            return 0;
        } else {
            return Integer.parseInt(str);
        }
    }

}
