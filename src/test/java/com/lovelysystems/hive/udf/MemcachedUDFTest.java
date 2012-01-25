package com.lovelysystems.hive.udf;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class MemcachedUDFTest {

    @Test
    public void testEvaluate() {
        MemcachedUDF udf = new MemcachedUDF();
        Text server = new Text("localhost:12345");
        assertEquals(-1, udf.evaluate(null, null, null).get());

        // we get errors (-2) because memcache is not running
        assertEquals(-2, udf.evaluate(server, new Text("A"), null).get());
        assertEquals(-2, udf
                .evaluate(server, new Text("A"), new Text("Hoschi")).get());

    }

}
