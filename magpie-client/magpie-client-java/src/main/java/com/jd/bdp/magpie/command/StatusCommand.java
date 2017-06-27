package com.jd.bdp.magpie.command;

import com.jd.bdp.magpie.utils.Constants;
import com.jd.bdp.magpie.utils.Utils;
import com.jd.bdp.magpie.utils.ZkUtils;
import com.jd.bdp.magpie.client.MainExecutor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-12-31
 * Time: 下午2:58
 * To change this template use File | Settings | File Templates.
 */
public class StatusCommand implements MainExecutor.ClientCommand {
    private static final Logger LOG = LoggerFactory.getLogger(StatusCommand.class);

    public StatusCommand() {
    }

    @Override
    public Options getOpts() {
        Options opts = new Options();
        opts.addOption("host", true, "the host of Thrift Server, if not supplied, it'll get from the zookeeper.");
        opts.addOption("port", true, "the port of Thrift Server, if not supplied, it'll get from the zookeeper.");
        opts.addOption("id", true, "the id of this task");
        return opts;
    }

    @Override
    public void process(CommandLine cl, @SuppressWarnings("rawtypes") Map config) throws Exception {
        List<String> zookeepers = (List<String>) config.get(Constants.ZKSERVERS);
        Integer port = (Integer) config.get(Constants.ZKPORT);
        if (port == null) {
            port = 2181;
        }
        for (int i = 0; i < zookeepers.size(); i++) {
            zookeepers.set(i, zookeepers.get(i) + ":" + port);
        }
        String zkServers = StringUtils.join(zookeepers, ",");
        ZkUtils zkUtils = new ZkUtils(zkServers, (String) config.get(Constants.ZKROOT));
        String id = cl.getOptionValue("id");
        if (id == null) {
            HelpFormatter f = new HelpFormatter();
            f.printHelp("Usage:", getOpts());
            throw new IOException("Parameters error!");
        }
        String status = null;
        byte[] statusData = zkUtils.getData(Utils.getStatusNode(id), false);
        if (statusData != null) {
            status = Utils.bytesToString(statusData);
        } else {
            status = "not running";
        }
        LOG.info(status);
        zkUtils.close();
    }
}
