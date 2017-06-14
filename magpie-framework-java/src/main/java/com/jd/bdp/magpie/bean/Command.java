package com.jd.bdp.magpie.bean;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 14-2-26
 * Time: 下午12:53
 * To change this template use File | Settings | File Templates.
 */
public enum Command {
    INIT("init", 1),
    RUN("run", 2),
    RELOAD("reload", 3),
    PAUSE("pause", 4),
    WAIT("wait", 5),
    KILL("kill", 6);

    private String value;
    private int id;

    public String getValue() {
        return this.value;
    }

    public int getId() {
        return this.id;
    }

    private Command(String value, int id) {
        this.value = value;
        this.id = id;
    }

    public static Command parseCommand(int id){
         switch (id){
             case 1:return INIT;
             case 2:return RUN;
             case 3:return RELOAD;
             case 4:return PAUSE;
             case 5:return WAIT;
             case 6:return KILL;
             default:return INIT;
         }
    }
}
