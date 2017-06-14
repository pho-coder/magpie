package com.jd.magpie.command;

import com.jd.magpie.client.MainExecutor;
import com.jd.magpie.utils.Constants;
import com.jd.magpie.utils.Utils;
import com.jd.magpie.utils.ZkUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-12-31
 * Time: 下午2:58
 * To change this template use File | Settings | File Templates.
 */
public class InfoCommand implements MainExecutor.ClientCommand {
    private static final Logger LOG = LoggerFactory.getLogger(InfoCommand.class);

    public InfoCommand() {
    }

    @Override
    public Options getOpts() {
        Options opts = new Options();
        opts.addOption("host", true, "Optional. the host of Thrift Server, if not supplied, it'll get from the zookeeper.");
        opts.addOption("port", true, "Optional. the port of Thrift Server, if not supplied, it'll get from the zookeeper.");
        opts.addOption("list", true, "Required. nimbuses, supervisors, assignments");
//        opts.addOption("id", true, "the id of this task");
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

        // list nodes
        if (cl.hasOption("list")) {
            String listWhich = cl.getOptionValue("list");
            if (!(listWhich.equals("nimbuses") || listWhich.equals("supervisors") || listWhich.equals("assignments"))) {
                HelpFormatter f = new HelpFormatter();
                f.printHelp("Usage:", getOpts());
                throw new IOException("Parameters error!");
            }
            if (listWhich.equals("nimbuses")) {
                LOG.info("nimbuses list:");
                boolean master = true;
                for (String nimbus : zkUtils.getChildren(Utils.getNimbusPath(), false)) {
                    if (master) {
                        LOG.info(nimbus + " : "
                                        + Utils.bytesToMap(zkUtils.getData(Utils.getNimbusPath() + "/" + nimbus, false)).get("ip")
                                        + " master");
                        master = false;
                    } else {
                        LOG.info(nimbus + " : "
                                        + Utils.bytesToMap(zkUtils.getData(Utils.getNimbusPath() + "/" + nimbus, false)).get("ip")
                                        + " backup");
                    }
                }
            } else if (listWhich.equals("supervisors")) {
                HashMap<String, HashMap> groupList = new HashMap<String, HashMap>();
                LOG.info("supervisors list:");
                for (String supervisor : zkUtils.getChildren(Utils.getSupervisorsPath(), false)) {
                    HashMap<String, Object> supervisorInfo = Utils.bytesToMap(zkUtils.getData(Utils.getSupervisorsPath() + "/" + supervisor, false));
                    HashMap<String, String> supervisorInfoNeeded = new HashMap<String, String>();
                    for (String k : supervisorInfo.keySet()) {
                        if (k.equals("group") || k.equals("ip")
                                              || k.equals("memory-score")
                                              || k.equals("cpu-score")
                                              || k.equals("net-bandwidth-score")) {
                            supervisorInfoNeeded.put(k, supervisorInfo.get(k).toString());
                        }
                    }
                    if (!(groupList.containsKey(supervisorInfoNeeded.get("group")))) {
                        HashMap<String, Integer> groupInfo = new HashMap<String, Integer>();
                        groupInfo.put("total-supervisor", 0);
                        groupInfo.put("total-supervisor-usable", 0);
                        groupInfo.put("total-memory-score", 0);
                        groupInfo.put("total-cpu-score", 0);
                        groupInfo.put("total-net-bandwidth-score", 0);
                        groupList.put(supervisorInfoNeeded.get("group"), groupInfo);
                    }
                    HashMap<String, Integer> groupInfo = groupList.get(supervisorInfoNeeded.get("group"));
                    groupInfo.put("total-supervisor", groupInfo.get("total-supervisor") + 1);
                    if (Utils.str2int(supervisorInfoNeeded.get("memory-score")) > 20 &&
                            Utils.str2int(supervisorInfoNeeded.get("cpu-score")) > 20 &&
                            Utils.str2int(supervisorInfoNeeded.get("net-bandwidth-score")) > 20) {
                        groupInfo.put("total-supervisor-usable", groupInfo.get("total-supervisor-usable") + 1);
                    }
                    groupInfo.put("total-memory-score",
                            groupInfo.get("total-memory-score") + Utils.str2int(supervisorInfoNeeded.get("memory-score")));
                    groupInfo.put("total-cpu-score",
                            groupInfo.get("total-cpu-score") + Utils.str2int(supervisorInfoNeeded.get("cpu-score")));
                    groupInfo.put("total-net-bandwidth-score",
                            groupInfo.get("total-net-bandwidth-score") + Utils.str2int(supervisorInfoNeeded.get("net-bandwidth-score")));

                    LOG.info(supervisor + " : "
                                        + "group(" + supervisorInfoNeeded.get("group") + ") "
                                        + "ip(" + supervisorInfoNeeded.get("ip") + ") "
                                        + "memory-score(" + supervisorInfoNeeded.get("memory-score") + ") "
                                        + "cpu-score(" + supervisorInfoNeeded.get("cpu-score") + ") "
                                        + "net-bandwidth-score(" + supervisorInfoNeeded.get("net-bandwidth-score") + ")");
                }

                LOG.info("groups info:");
                for (String group : groupList.keySet()) {
                    HashMap<String, Integer> groupInfo = groupList.get(group);
                    LOG.info(group + " :"
                    + " total-supervisor(" + groupInfo.get("total-supervisor") + ")"
                    + " total-supervisor-usable(" + groupInfo.get("total-supervisor-usable") + ")"
                    + " avg-memory-score(" + groupInfo.get("total-memory-score") / groupInfo.get("total-supervisor") + ")"
                    + " avg-cpu-score(" + groupInfo.get("total-cpu-score") / groupInfo.get("total-supervisor") + ")"
                    + " avg-net-bandwidth-score(" + groupInfo.get("total-net-bandwidth-score") / groupInfo.get("total-supervisor") + ")");
                }
            } else if (listWhich.equals("assignments")) {
                LOG.info("assignments list:");
                for (String assignment : zkUtils.getChildren(Utils.getAssignmentsPath(), false)) {
                    HashMap<String, Object> assignmentInfo = Utils.bytesToMap(zkUtils.getData(Utils.getAssignmentsPath() + "/" + assignment, false));
                    LOG.info(assignment + ":"
                    + " group(" + assignmentInfo.get("group") + ")"
                    + " supervisor(" + assignmentInfo.get("supervisor") + ")"
                    + " start-time(" + new Timestamp(Long.parseLong(assignmentInfo.get("start-time").toString())).toString() + ")");
                }
            }
        }

        zkUtils.close();
    }
}
