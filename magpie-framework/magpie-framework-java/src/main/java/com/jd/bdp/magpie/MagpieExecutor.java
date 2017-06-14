package com.jd.bdp.magpie;

/**
 * Created by darwin on 5/15/14.
 */
public interface MagpieExecutor {
    /**
     * 初始化application，任务里初始化的操作都在这里执行，比如queue的连接、系统的配置等
     * 任务在第一次开始执行{@link #run()}前，会先执行这个方法。
     * @param id 这个是任务id，可以通过它来获取配置
     * @throws Exception
     */
    public void prepare(String id) throws Exception;

    /**
     * 当对系统任务执行reload命令时，会调用这个方法
     * @param id
     * @throws Exception
     */
    public void reload(String id) throws Exception;

    /**
     * 当对系统任务执行pause命令时，会调用这个方法
     * @param id
     * @throws Exception
     */
    public void pause(String id) throws Exception;

    /**
     * 当对系统任务执行kill命令时，会调用这个方法
     * @param id
     * @throws Exception
     */
    public void close(String id) throws Exception;

    /**
     * 这个是系统正常执行的方法，它会被连续无休眠地被调用，即当它执行结束后，这个方法就会被再次调用，直到收到其它命令。
     * 为了保证任务能收到系统的其它指令，这个方法里最好不要有耗时操作，更不可在此执行死循环。
     * @throws Exception
     */
    public void run() throws Exception;
}
