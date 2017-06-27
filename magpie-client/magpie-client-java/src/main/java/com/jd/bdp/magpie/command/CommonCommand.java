package com.jd.bdp.magpie.command;

import com.jd.bdp.magpie.client.MagpieClient;
import com.jd.bdp.magpie.client.MainExecutor;
import com.jd.bdp.magpie.generated.Nimbus;
import com.jd.bdp.magpie.utils.Constants;
import com.jd.bdp.magpie.utils.Utils;
import com.jd.bdp.magpie.utils.ZkUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-12-25
 * Time: 下午5:23
 * To change this template use File | Settings | File Templates.
 */
public class CommonCommand implements MainExecutor.ClientCommand {
    private static final Logger LOG = LoggerFactory.getLogger(CommonCommand.class);
    final String command;

    public CommonCommand(String command) {
        this.command = command;
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
        List<String> nimbuses = zkUtils.getChildren(Utils.getNimbusPath());
        if (nimbuses.size() < 1) {
            LOG.info("There isn't any nimbus running now! Please try it later..");
            return;
        }
        String[] nodes = nimbuses.toArray(new String[nimbuses.size()]);
        Arrays.sort(nodes);
        String nimbus = nodes[0];
        LOG.info("Nimbus: " + nimbus);
        byte[] nimbusBytes = zkUtils.getData(Utils.getNimbusPath() + "/" + nimbus, true);
        HashMap<String, Object> nimbusInfo = Utils.bytesToMap(nimbusBytes);
        MagpieClient magpieClient = new MagpieClient(config, (String) nimbusInfo.get("ip"), (Integer) nimbusInfo.get("port"));
        Nimbus.Client client = magpieClient.getClient();
        String id = cl.getOptionValue("id");
        if (id == null) {
            HelpFormatter f = new HelpFormatter();
            f.printHelp("Usage:", getOpts());
            throw new IOException("Parameters error!");
        }
        String result = null;
        if (command.equals("pause")) {
            result = client.pauseTopology(id);
        } else if (command.equals("active")) {
            result = client.activeTopology(id);
        } else if (command.equals("reload")) {
            result = client.reloadTopology(id);
        }
        LOG.info(result);
        zkUtils.close();
    }
}
