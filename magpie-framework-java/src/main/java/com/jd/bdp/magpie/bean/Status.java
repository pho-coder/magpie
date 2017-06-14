package com.jd.bdp.magpie.bean;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 14-2-26
 * Time: 下午12:53
 * To change this template use File | Settings | File Templates.
 */
public enum Status {
    RELOADED("reloaded"),
    KILLED("killed"),
    RUNNING("running"),
    PAUSED("paused");
    private String value;
    public String getValue(){
        return this.value;
    }
    private Status(String value){
        this.value = value;
    }
}
