package com.jd.magpie.client;

import com.jd.magpie.generated.Nimbus;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: caishize
 * Date: 14-2-24
 * Time: 下午1:10
 * To change this template use File | Settings | File Templates.
 */
public class MagpieClient extends com.jd.magpie.auth.ThriftClient {
    private Nimbus.Client _client;

    public MagpieClient(Map conf, String host, int port) throws TTransportException {
        super(conf, host, port);
        _client = new Nimbus.Client(_protocol);
    }

    public Nimbus.Client getClient() {
        return _client;
    }

    public static void main(String[] args) throws TException {
        String host = "10.12.218.221";
        int port = 6666;
        MagpieClient magpieClient = new MagpieClient(new HashMap(), host, port);
        magpieClient.getClient().submitTopology("mysql", "tracker", "1025");
        magpieClient.close();
    }
}
