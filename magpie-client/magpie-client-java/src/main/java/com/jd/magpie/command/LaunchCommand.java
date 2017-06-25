package com.jd.magpie.command;

import com.jd.magpie.client.MagpieClient;
import com.jd.magpie.utils.Constants;
import com.jd.magpie.utils.Utils;
import com.jd.magpie.utils.ZkUtils;
import com.jd.magpie.client.MainExecutor;
import com.jd.magpie.generated.Nimbus;
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
 * Modifer: phoenix
 * Date: 2014-10-08
 */
public class LaunchCommand implements MainExecutor.ClientCommand {
    private static final Logger LOG = LoggerFactory.getLogger(LaunchCommand.class);
    final String command;

    public LaunchCommand(String command) {
        this.command = command;
    }

    @Override
    public Options getOpts() {
        Options opts = new Options();
        opts.addOption("host", true, "Optional. the host of Thrift Server, if not supplied, it'll get from the zookeeper.");
        opts.addOption("port", true, "Optional. the port of Thrift Server, if not supplied, it'll get from the zookeeper.");
        opts.addOption("id", true, "the id of this task");
        opts.addOption("jar", true, "the jar name of this task");
        opts.addOption("class", true, "the class that will be called");
        opts.addOption("group", true, "Optional. the group where the job will be submitted to. group default is default.");
        opts.addOption("type", true, "Optional. the type which the job is, including memory, cpu, network. memory is default.");
        opts.addOption("d", false, "Optional. the debug mode will print more info.");
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
        String jar = cl.getOptionValue("jar");
        String klass = cl.getOptionValue("class");
        String id = cl.getOptionValue("id");
        String group = cl.getOptionValue("group");
        String type = cl.getOptionValue("type");
        if (jar == null || klass == null || id == null) {
            HelpFormatter f = new HelpFormatter();
            f.printHelp("Usage:", getOpts());
            throw new IOException("Parameters error!");
        }
        String result = "";
        if (group == null && type == null) {
            result = client.submitTopology(id, jar, klass);
        } else {
            if (group == null) {
                group = Constants.TASK_GROUP_DEFAULT;
            }
            if (type == null) {
                type = Constants.TASK_TYPE_DEFAULT;
            }
            if (!(type.equals(Constants.TASK_TYPE_MEMORY) || type.equals(Constants.TASK_TYPE_CPU) || type.equals(Constants.TASK_TYPE_NETWORK))) {
                HelpFormatter f = new HelpFormatter();
                f.printHelp("Usage:", getOpts());
                throw new IOException("parameter TYPE error! type must be memory, cpu or network!");
            }
            result = client.submitTask(id, jar, klass, group, type);
        }
        LOG.info(result);

        if (cl.hasOption("d")) {
            LOG.info("debug mode");
            int times = 30;
            while (true) {
                if (times == 0) {
                    LOG.info("time elapses 60s!, the task " + id + " hasn't submitted!");
                    break;
                }
                byte[] assignmentBytes = zkUtils.getData(Utils.getAssignmentsPath() + "/" + id, false);
                if (assignmentBytes == null) {
                    LOG.info("assignments node not exists!");
                    times --;
                    Thread.sleep(2000);
                    continue;
                } else {
                    HashMap<String, Object> assignmentInfo = Utils.bytesToMap(assignmentBytes);
                    if (assignmentInfo.containsKey("supervisor")) {
                        LOG.info("the task " + id + " has been submitted to supervisor: " + assignmentInfo.get("supervisor"));
                        break;
                    } else {
                        LOG.info("the task " + id + " hasn't been submitted!");
                        times --;
                        Thread.sleep(2000);
                        continue;
                    }
                }
            }
        }
        zkUtils.close();
    }


}
