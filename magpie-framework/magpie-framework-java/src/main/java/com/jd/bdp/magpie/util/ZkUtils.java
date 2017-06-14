package com.jd.bdp.magpie.util;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-12-17
 * Time: 上午10:03
 * To change this template use File | Settings | File Templates.
 */
public class ZkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ZkUtils.class);
    private static CuratorFramework client = null;
    private final String zkconnectString;
    private final String zkpath;

    public ZkUtils(String zkconnectString, String zkpath) {
        this.zkconnectString = zkconnectString;
        this.zkpath = zkpath;
    }

    public void connect() {
        if (client == null || client.getState() != CuratorFrameworkState.STARTED) {
            CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
            String zkroot = zkpath != null ? zkpath : "/magpie";
            client = builder.connectString(zkconnectString + zkroot)
                    .connectionTimeoutMs(15000)
                    .sessionTimeoutMs(20000)
                    .retryPolicy(new BoundedExponentialBackoffRetry(1000, 5, 30000))
                    .build();
            CuratorListener listener = new CuratorListener() {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                    if (event == null) {
                        return;
                    }
                    if (CuratorEventType.CLOSING == event.getType()) {
                        LOG.info("Zookeeper Client will be closed!");
                    } else {
//                        LOG.debug("CuratorEvent type:" + event.getType().toString());
//                        LOG.debug("CuratorEvent path:" + event.getPath());
                    }
                }
            };
            client.getCuratorListenable().addListener(listener);
            client.start();
            client.sync();
        }
    }

    public boolean createNode(String path, byte[] data, CreateMode createMode) throws Exception {
        connect();
        try {
            if (createMode == null) {
                createMode = CreateMode.EPHEMERAL;
            }
            client.create().creatingParentsIfNeeded().withMode(createMode).forPath(path, data);
            return true;
        } catch (Exception e) {
            if (e.getClass() == KeeperException.NodeExistsException.class) {
                return false;
            } else {
                LOG.error(ExceptionUtils.getFullStackTrace(e));
//                e.printStackTrace();
                LOG.error(e.toString());//报警
                throw e;
            }
        }
    }

    public byte[] getData(String path) throws Exception {
        return getData(path, false);
    }

    public byte[] getData(String path, boolean waitUtilExists) throws Exception {
        connect();
        try {
            byte[] value = client.getData().forPath(path);
            return value;
        } catch (Exception e) {
//            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            LOG.error(e.toString());//报警;
            if (e.getClass() == KeeperException.NoNodeException.class) {
                if (waitUtilExists) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e1) {
                    }
                    return getData(path, waitUtilExists);
                }
            } else {
                throw e;
            }
        }
        return null;
    }

    public void setData(String path, byte[] data) throws Exception {
        setData(path, data, true);
    }

    public void setData(String path, byte[] data, boolean createIfnotExist) throws Exception {
        connect();
        try {
            client.setData().forPath(path, data);
        } catch (Exception e) {
//            e.printStackTrace();
            LOG.error(ExceptionUtils.getFullStackTrace(e));
            LOG.error(e.toString());//报警;
            if (e.getClass() == KeeperException.NoNodeException.class) {
                if (createIfnotExist) {
                    createNode(path, data, CreateMode.PERSISTENT);
                }
            } else {
                throw e;
            }
        }
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }


}
