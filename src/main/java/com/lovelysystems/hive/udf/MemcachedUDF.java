package com.lovelysystems.hive.udf;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Future;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.MemcachedClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = false)
@Description(name = "memcached",
        value = "_FUNC_(servers, key, value) - Sets the given key in Memcached")
public class MemcachedUDF extends UDF {

    static final Log LOG = LogFactory.getLog(MemcachedUDF.class.getName());

    protected final HashMap<Text, MemcachedClient> clients = new HashMap<Text, MemcachedClient>();
    private final IntWritable result = new IntWritable(-1);
    private static final DefaultConnectionFactory defaultConnFactory = new DefaultConnectionFactory(
            DefaultConnectionFactory.DEFAULT_OP_QUEUE_LEN,
            DefaultConnectionFactory.DEFAULT_READ_BUFFER_SIZE,
            HashAlgorithm.CRC32_HASH);

    private MemcachedClient getClient(Text servers) throws IOException {
        if (!clients.containsKey(servers)) {
            MemcachedClient client = new MemcachedClient(defaultConnFactory,
                    AddrUtil.getAddresses(servers.toString()));
            clients.put(servers, client);
            return client;
        } else {
            return clients.get(servers);
        }
    }

    public IntWritable evaluate(Text servers, Text key, Text value) {
        if (key == null || servers == null) {
            result.set(-1);
            return result;
        }
        MemcachedClient client;
        try {
            client = getClient(servers);
        } catch (IOException e) {
            LOG.error("failed to get client: servers=" + servers + " key="
                    + key + " value=" + value, e);
            result.set(-1);
            return result;
        }
        Future<Boolean> f;
        if (value == null) {
            f = client.delete(key.toString());
            result.set(2);
        } else {

            f = client.set(key.toString(), 0, value.toString());
            result.set(1);
        }
        try {
            f.get();
        } catch (Exception e) {
            LOG.error("failed to set value: servers=" + servers + " key=" + key
                    + " value=" + value, e);
            result.set(-2);
        }
        return result;
    }

}
