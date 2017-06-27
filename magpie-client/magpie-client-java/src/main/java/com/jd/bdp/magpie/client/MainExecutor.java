package com.jd.bdp.magpie.client;

import com.jd.bdp.magpie.command.*;
import com.jd.bdp.magpie.utils.Utils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-12-25
 * Time: 下午4:55
 * To change this template use File | Settings | File Templates.
 */
public class MainExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(MainExecutor.class);

    public static interface ClientCommand {

        /**
         * @return the options this client will process.
         */
        public Options getOpts();

        /**
         * Do the processing
         *
         * @param cl          the arguments to process
         * @param config the plumber configuration to use
         * @throws Exception on any error
         */
        public void process(CommandLine cl,
                            @SuppressWarnings("rawtypes") Map config) throws Exception;
    }

    public static class HelpCommand implements ClientCommand {
        HashMap<String, ClientCommand> _commands;

        public HelpCommand(HashMap<String, ClientCommand> commands) {
            _commands = commands;
        }

        @Override
        public Options getOpts() {
            return new Options();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(CommandLine cl,
                            @SuppressWarnings("rawtypes") Map ignored) throws Exception {
            printHelpFor(cl.getArgList());
        }

        public void printHelpFor(Collection<String> args) {
            if (args == null || args.size() < 1) {
                args = _commands.keySet();
            }
            HelpFormatter f = new HelpFormatter();
            for (String command : args) {
                ClientCommand c = _commands.get(command);
                if (c != null) {
                    f.printHelp(command, c.getOpts());
                } else {
                    System.err.println("ERROR: " + c + " is not a supported command.");
                    //TODO make this exit with an error at some point
                }
            }
        }
    }

    public void execute(String[] args) throws Exception {
        HashMap<String, ClientCommand> commands = new HashMap<String, ClientCommand>();
        HelpCommand help = new HelpCommand(commands);
        commands.put("help", help);
        commands.put("submit", new LaunchCommand("submit"));
        commands.put("kill", new KillCommand("kill"));
        commands.put("pause", new CommonCommand("pause"));
        commands.put("active", new CommonCommand("active"));
        commands.put("reload", new CommonCommand("reload"));
        commands.put("getStatus", new StatusCommand());
        commands.put("setWebService", new ConfigCommand());
        commands.put("info", new InfoCommand());

        String commandName = null;
        String[] commandArgs = null;
        if (args.length < 1) {
            commandName = "help";
            commandArgs = new String[0];
        } else {
            commandName = args[0];
            commandArgs = Arrays.copyOfRange(args, 1, args.length);
        }

        ClientCommand command = commands.get(commandName);
        if (command == null) {
            LOG.error("ERROR: " + commandName + " is not a supported command.");
            help.printHelpFor(null);
            System.exit(1);
        }
        Options opts = command.getOpts();
        if (!opts.hasOption("h")) {
            opts.addOption("h", "help", false, "print out a help message");
        }

        CommandLine cl = new GnuParser().parse(opts, commandArgs);
        if (cl.hasOption("help")) {
            help.printHelpFor(Arrays.asList(commandName));
        } else {
            Map config = Utils.readConfig("magpie.yaml");
            command.process(cl, config);
        }
    }

    public static void main(String[] args) throws Exception {
//        args = new String[]{
//                "setWebService",
//                "-log",
//                "http://172.17.36.62:8080/dataReceiver/front/tranxLogBatch",
//                "-meta",
//                "http://172.17.36.62:8080/dataReceiver/front/tconf/",
////                "-host",
////                "192.168.137.123",
////                "-port",
////                "1433",
////                "-database",
////                "darwin",
////                "-status",
////                "offline"
//        };
        MainExecutor executor = new MainExecutor();
        executor.execute(args);
    }
}
