package com.jd.magpie.auth;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-11-20
 * Time: 下午4:19
 * To change this template use File | Settings | File Templates.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.URIParameter;
import java.util.Map;

public class AuthUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuthUtils.class);
    /**
     * Construct a JAAS configuration object per storm configuration file
     * @param magpie_conf magpie configuration
     * @return JAAS configuration object
     */
    public static Configuration GetConfiguration(Map magpie_conf) {
        Configuration login_conf = null;

        //find login file configuration from Storm configuration
        String loginConfigurationFile = (String)magpie_conf.get("java.security.com.jd.magpie.auth.login.config");
        if ((loginConfigurationFile != null) && (loginConfigurationFile.length()>0)) {
            try {
                URI config_uri = new File(loginConfigurationFile).toURI();
                login_conf = Configuration.getInstance("JavaLoginConfig", new URIParameter(config_uri));
            } catch (NoSuchAlgorithmException ex1) {
                if (ex1.getCause() instanceof FileNotFoundException)
                    throw new RuntimeException("configuration file "+loginConfigurationFile+" could not be found");
                else throw new RuntimeException(ex1);
            } catch (Exception ex2) {
                throw new RuntimeException(ex2);
            }
        }

        return login_conf;
    }

    /**
     * Construct a transport plugin per storm configuration
     * @param magpie_conf plumber configuration
     * @return
     */
    public static ITransportPlugin GetTransportPlugin(Map magpie_conf, Configuration login_conf) {
        ITransportPlugin  transportPlugin = null;
        try {
            String transport_plugin_klassName = "com.jd.magpie.auth.SimpleTransportPlugin";
            Class klass = Class.forName(transport_plugin_klassName);
            transportPlugin = (ITransportPlugin)klass.newInstance();
            transportPlugin.prepare(login_conf);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        return transportPlugin;
    }

    public static String get(Configuration configuration, String section, String key) throws IOException {
        AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(section);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '"+ section + "' entry in this configuration.";
            throw new IOException(errorMessage);
        }

        for(AppConfigurationEntry entry: configurationEntries) {
            Object val = entry.getOptions().get(key);
            if (val != null)
                return (String)val;
        }
        return null;
    }
}
