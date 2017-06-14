package com.jd.bdp.magpie.util;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-12-13
 * Time: 下午3:37
 * To change this template use File | Settings | File Templates.
 */
public class Config {

    public static String getCommandPath(String node) {
        return "/commands/" + node;
    }

    public static String getStatusPath(String node) {
        return "/status/" + node;
    }

    public static String getHeartbeatPath(String node) {
        return "/workerbeats/" + node;
    }

}
