package com.jd.magpie.auth;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 13-11-20
 * Time: 下午3:27
 * To change this template use File | Settings | File Templates.
 */

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.security.auth.login.Configuration;
import java.io.IOException;

/**
 * Interface for Thrift Transport plugin
 */
public interface ITransportPlugin {
    /**
     * Invoked once immediately after construction
     * @param login_conf login configuration
     */
    void prepare(Configuration login_conf);

    /**
     * Create a server associated with a given port and service handler
     * @param port listening port
     * @param processor service handler
     * @return server to be binded
     */
    public TServer getServer(int port, TProcessor processor) throws IOException, TTransportException;

    /**
     * Connect to the specified server via framed transport
     * @param transport The underlying Thrift transport.
     * @param serverHost server host
     */
    public TTransport connect(TTransport transport, String serverHost) throws IOException, TTransportException;
}