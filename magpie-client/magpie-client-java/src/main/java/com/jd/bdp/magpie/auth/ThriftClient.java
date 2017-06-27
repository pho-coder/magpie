package com.jd.bdp.magpie.auth;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-11-22
 * Time: 上午11:07
 * To change this template use File | Settings | File Templates.
 */

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.Map;

public class ThriftClient {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftClient.class);
    private TTransport _transport;
    protected TProtocol _protocol;

    public ThriftClient(Map storm_conf, String host, int port) throws TTransportException {
        this(storm_conf, host, port, null);
    }

    public ThriftClient(Map storm_conf, String host, int port, Integer timeout) throws TTransportException {
        try {
            //locate login configuration
            Configuration login_conf = AuthUtils.GetConfiguration(storm_conf);

            //construct a transport plugin
            ITransportPlugin transportPlugin = AuthUtils.GetTransportPlugin(storm_conf, login_conf);

            //create a socket with server
            if(host==null) {
                throw new IllegalArgumentException("host is not set");
            }
            if(port<=0) {
                throw new IllegalArgumentException("invalid port: "+port);
            }
            TSocket socket = new TSocket(host, port);
            if(timeout!=null) {
                socket.setTimeout(timeout);
            }
            final TTransport underlyingTransport = socket;

            //establish client-server transport via plugin
            _transport =  transportPlugin.connect(underlyingTransport, host);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        _protocol = null;
        if (_transport != null)
            _protocol = new  TBinaryProtocol(_transport);
    }

    public TTransport transport() {
        return _transport;
    }

    public void close() {
        _transport.close();
    }
}
