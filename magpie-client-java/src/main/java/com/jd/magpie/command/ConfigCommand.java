package com.jd.magpie.command;

import com.jd.magpie.utils.Constants;
import com.jd.magpie.utils.Utils;
import com.jd.magpie.utils.ZkUtils;
import com.jd.magpie.client.MainExecutor;
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
 * Date: 13-12-25
 * Time: 下午5:23
 * To change this template use File | Settings | File Templates.
 */
public class ConfigCommand implements MainExecutor.ClientCommand {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigCommand.class);


    @Override
    public Options getOpts() {
        Options opts = new Options();
        opts.addOption("resource", true, "the web service url that can get the application jars from this address");
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
        String resourceAddress = cl.getOptionValue("resource");
        if (resourceAddress != null) {
            zkUtils.setData(Utils.getResourceWebServiceZkPath(), Utils.stringToBytes(resourceAddress), false);
            LOG.info("set Web Service: \n\tdownload application jars address: " + resourceAddress);
        }

        if (resourceAddress == null) {
            HelpFormatter f = new HelpFormatter();
            f.printHelp("Usage:", getOpts());
            throw new IOException("Parameters error!");
        }
        zkUtils.close();
    }
}
