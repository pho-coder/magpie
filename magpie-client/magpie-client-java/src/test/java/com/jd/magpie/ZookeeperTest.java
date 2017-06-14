package com.jd.magpie;


import com.jd.magpie.utils.Utils;
import com.jd.magpie.utils.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 14-3-11
 * Time: 下午6:48
 * To change this template use File | Settings | File Templates.
 */
public class ZookeeperTest {
    public void testSeqNode() throws Exception {
        ZkUtils zkUtils = new ZkUtils("storm1:2181","/magpie-test");
        zkUtils.createNode("/abc/nimbus1", Utils.stringToBytes("abc"), CreateMode.EPHEMERAL_SEQUENTIAL);
        zkUtils.createNode("/abc/nimbus2", Utils.stringToBytes("abc"), CreateMode.EPHEMERAL_SEQUENTIAL);
        List<String> list = zkUtils.getChildren("/abc");
        String[] nodes = list.toArray(new String[list.size()]);
        Arrays.sort(nodes);
        String mynode = "/nimbus1";
        if(nodes[0].equals(mynode)){
            System.out.println(mynode);
        }
        System.out.println(list);
        Thread.sleep(10000);
    }
}
