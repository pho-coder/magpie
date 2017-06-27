package com.jd.bdp.magpie.auth;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-11-20
 * Time: 下午3:38
 * To change this template use File | Settings | File Templates.
 */

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.util.Map;

public class ThriftServer {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class);
    private Map _magpie_conf; //plumber configuration
    protected TProcessor _processor = null;
    private int _port = 0;
    private TServer _server = null;
    private Configuration _login_conf;

    public ThriftServer(Map magpie_conf, TProcessor processor, int port) {
        try {
            _magpie_conf = magpie_conf;
            _processor = processor;
            _port = port;

            //retrieve authentication configuration
            _login_conf = AuthUtils.GetConfiguration(_magpie_conf);
        } catch (Exception x) {
            LOG.error(x.getMessage(), x);
        }
    }

    public void stop() {
        if (_server != null)
            _server.stop();
    }

    /**
     * Is ThriftServer listening to requests?
     * @return
     */
    public boolean isServing() {
        if (_server == null) return false;
        return _server.isServing();
    }



    public void serve()  {
        try {
            //locate our thrift transport plugin
            ITransportPlugin transportPlugin = AuthUtils.GetTransportPlugin(_magpie_conf, _login_conf);

            //server
            _server = transportPlugin.getServer(_port, _processor);

            //start accepting requests
            _server.serve();
        } catch (Exception ex) {
            LOG.error("ThriftServer is being stopped due to: " + ex, ex);
            if (_server != null) _server.stop();
            Runtime.getRuntime().halt(1); //shutdown server process since we could not handle Thrift requests any more
        }
    }
}
