package com.lovelysystems.hive.udf;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ESHashUDFTest {

    @Test
    public void testEvaluate() {
        ESHashUDF udf = new ESHashUDF();

        assertEquals(177622, udf.evaluate(new Text("1")).get());
        assertEquals(-750880694, udf.evaluate(new Text("1212351342513451451"))
                .get());
        assertEquals(193485963, udf.evaluate(new Text("abc")).get());
        assertEquals(-2147483648, udf.evaluate(new Text("57744018578214912"))
                .get());

    }

}
