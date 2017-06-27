package com.jd.bdp.magpie.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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

            String namespace = zkpath != null ? zkpath : "magpie";
            if (namespace.startsWith("/")) {
                namespace = namespace.substring(1);
            }
            client = builder.connectString(zkconnectString)
                    .sessionTimeoutMs(30000)
                    .connectionTimeoutMs(30000)
                    .canBeReadOnly(false)
                    .retryPolicy(new ExponentialBackoffRetry(1000, Integer.MAX_VALUE))
                    .namespace(namespace)
                    .defaultData(null)
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
                e.printStackTrace();
                LOG.error(e.toString());//报警

                LOG.error("JRDW Zookeeper Warn!", "Create zknode failed! Please check! \nException:" + e.toString());
                throw e;
            }
        }
    }

    public boolean setData(String path, byte[] data, boolean mustExist) throws Exception {
        connect();
        try {
            client.setData().forPath(path, data);
            return true;
        } catch (Exception e) {
            if (e.getClass() == KeeperException.NoNodeException.class) {
                if (mustExist) {
                    throw e;
                } else {
                    return createNode(path, data, CreateMode.PERSISTENT);
                }
            } else {
                e.printStackTrace();
                LOG.error(e.toString());//报警
                LOG.error("JRDW Zookeeper Warn!", "Create zknode failed! Please check! \nException:" + e.toString());
                throw e;
            }
        }
    }

    public void deleteNode(String path) throws Exception {
        connect();
        try {
            client.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            if (e.getClass() != KeeperException.NoNodeException.class) {
                e.printStackTrace();
                LOG.error(e.toString());//报警
                LOG.error("JRDW Zookeeper Warn!", "Delete zknode failed! Please check! \nException:" + e.toString());
                throw e;
            }
        }
    }

    public byte[] getData(String path) throws Exception {
        return getData(path, true);
    }

    public byte[] getData(String path, boolean waitUtilExists) throws Exception {
        connect();
        try {
            byte[] value = client.getData().forPath(path);
            return value;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.toString());//报警;
            LOG.error("JRDW Zookeeper Warn!", "Get zknode data failed! Please check! \nException:" + e.toString());
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

    public List<String> getChildren(String path) throws Exception {
        return getChildren(path, false);
    }

    public List<String> getChildren(String path, boolean waitUtilExists) throws Exception {
        connect();
        try {
            return client.getChildren().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.toString());//报警;
            LOG.error("JRDW Zookeeper Warn!", "Get zknode data failed! Please check! \nException:" + e.toString());
            if (e.getClass() == KeeperException.NoNodeException.class) {
                if (waitUtilExists) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e1) {
                    }
                    return getChildren(path, waitUtilExists);
                }
            } else {
                throw e;
            }
        }
        return null;
    }

    public boolean checkExists(String path) throws Exception {
        connect();
        try {
            return (client.checkExists().forPath(path) != null) ? true : false;
        } catch (Exception e) {
            LOG.error(e.toString());
            throw e;
        }
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }


}
