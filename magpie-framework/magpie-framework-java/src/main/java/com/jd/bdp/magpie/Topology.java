package com.jd.bdp.magpie;

import com.jd.bdp.magpie.bean.Command;
import com.jd.bdp.magpie.bean.Status;
import com.jd.bdp.magpie.util.Config;
import com.jd.bdp.magpie.util.Utils;
import com.jd.bdp.magpie.util.ZkUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.Throwable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-12-17
 * Time: 下午1:51
 * To change this template use File | Settings | File Templates.
 */
public class Topology {
    private static final Logger LOG = LoggerFactory.getLogger(Topology.class);
    private static Timer commandTimer = new Timer(true);
    final public static int INITIAL = -1;
    private MagpieExecutor magpieExecutor;

    public Topology(MagpieExecutor magpieExecutor) {
        this.magpieExecutor = magpieExecutor;
    }

    public static void setStatus(ZkUtils zkUtils, String jobNode, Status status) throws Exception {
        zkUtils.setData(Config.getStatusPath(jobNode), Utils.stringToBytes(status.getValue()));
    }

    public static AtomicInteger checkCommand(final ZkUtils zkUtils, final String jobNode) {
        final AtomicInteger action = new AtomicInteger(INITIAL);
        commandTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    String command = (String) Utils.bytesToMap(zkUtils.getData(Config.getCommandPath(jobNode))).get("command");
                    if (command.equals(Command.INIT.getValue())) {
                        action.set(Command.INIT.getId());
                    } else if (command.equals(Command.KILL.getValue())) {
                        action.set(Command.KILL.getId());
                    } else if (command.equals(Command.PAUSE.getValue())) {
                        action.set(Command.PAUSE.getId());
                    } else if (command.equals(Command.RELOAD.getValue())) {
                        action.set(Command.RELOAD.getId());
                    } else if (command.equals(Command.RUN.getValue())) {
                        action.set(Command.RUN.getId());
                    } else if (command.equals(Command.WAIT.getValue())) {
                        action.set(Command.WAIT.getId());
                    } else {
                        LOG.error("unknown command:\t" + command);
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                    LOG.error(ExceptionUtils.getFullStackTrace(e));
                    LOG.error("Error accurs in checking command from zookeeper, maybe connection is lost!");
                    System.exit(-1);
                }
            }
        }, 1, 3);
        return action;
    }

//    public static Map getConfig(ZkUtils zkUtils, String jobId, String node) throws Exception {
//        String metaTransferAddress = Config.getMetaTransferAddress(zkUtils);
//        LOG.debug("try to get subscribe info from web service: " + metaTransferAddress);
//        Map conf = Config.getConfig(jobId, metaTransferAddress);
//        if (conf == null) {
//            throw new IOException("subscribe info can not be null,please check the web service");
//        }
//        LOG.debug("get subscribe info from web service: " + metaTransferAddress + " successfully!");
//        return conf;
//    }

    public void run() {
        String zkServers = System.getProperty("zookeeper.servers");
        String zkRoot = System.getProperty("zookeeper.root");
        String pidsDir = System.getProperty("pids.dir");
        String jobId = System.getProperty("job.id");
        String jobNode = System.getProperty("job.node");
        File file = new File(pidsDir);
        if (!file.isDirectory()) {
            file.mkdirs();
        }
        File pidFile = new File(file, Utils.getPid());
        try {
            pidFile.createNewFile();
        } catch (IOException e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            System.exit(-1);
        }
        ZkUtils zkUtils = null;
        try {
            zkUtils = new ZkUtils(zkServers, zkRoot);
            zkUtils.connect();
            while (!zkUtils.createNode(Config.getHeartbeatPath(jobNode), Utils.stringToBytes(""), CreateMode.EPHEMERAL)) {
                LOG.warn("node exists: " + Config.getHeartbeatPath(jobNode));
                Thread.sleep(1000);
            }
            setStatus(zkUtils, jobNode, Status.RUNNING);
//            Map conf = getConfig(zkUtils, jobId, jobNode);
            AtomicInteger action = checkCommand(zkUtils, jobNode);
            boolean hasReset = false;
            whiles:
            while (action.get() != Command.KILL.getId()) {
                Command command = Command.parseCommand(action.get());
                LOG.debug(command.toString());
                switch (command) {
                    case RELOAD:
                        if (!hasReset) {
                            hasReset = true;
//                            conf = getConfig(zkUtils, jobId, jobNode);
                            magpieExecutor.reload(jobId);
                            setStatus(zkUtils, jobNode, Status.RELOADED);
                        } else {
                            Thread.sleep(5000);
                        }
                        break;
                    case RUN:
                        hasReset = false;
                        try {
                            LOG.debug("start running");
                            setStatus(zkUtils, jobNode, Status.RUNNING);
                            magpieExecutor.prepare(jobId);
                            while (action.get() == Command.RUN.getId()) {
                                magpieExecutor.run();
                            }
                        } catch (Exception e) {
                            LOG.error("error accurs in running process");
                            LOG.error(ExceptionUtils.getFullStackTrace(e));
                            throw new RuntimeException(e);
                        } finally {
                            LOG.info("end running");
                            magpieExecutor.close(jobId);
                        }
                        break;
                    case INIT:
                        break;
                    case PAUSE:
                        hasReset = false;
                        magpieExecutor.pause(jobId);
                        setStatus(zkUtils, jobNode, Status.PAUSED);
                        Thread.sleep(1000);
                        break;
                    case WAIT:
                        LOG.warn("I got a wait command, and I'll exit!");
                        break whiles;
                    case KILL:
                        LOG.warn("I got a kill command, and I'll exit!");
                        magpieExecutor.close(jobId);
                        setStatus(zkUtils, jobNode, Status.KILLED);
                        break whiles;
                    default:
                        break;
                }
                Thread.sleep(5000);
            }
        } catch (Throwable e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
            LOG.info("This magpie app will be closed");
            commandTimer.cancel();
            System.exit(0);
        }
    }
}
