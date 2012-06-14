package com.lovelysystems.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class GreatestUDFTest {

    class DJO implements GenericUDF.DeferredObject {

        private Object v;

        public DJO(Object v) {
            this.v = v;
        }

        @Override
        public Object get() throws HiveException {
            return v;
        }

    }


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
    * Objects to compare must be the same type
    */
    @Test
    public void testInitialize() throws UDFArgumentException {

        thrown.expect(UDFArgumentException.class);
        thrown.expectMessage("The expressions after GREATEST should all have the same type: \"int\" is expected but \"string\" is found");

        GreatestUDF udf = new GreatestUDF();

        SettableIntObjectInspector firstInt = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        SettableStringObjectInspector firstString = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        udf.initialize(new ObjectInspector[] { firstInt, firstString });
    }

    /*
     * Comparing integers
     */
    @Test
    public void testEvaluateWithInt() {
        GreatestUDF udf = new GreatestUDF();


        SettableIntObjectInspector firstInt = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        SettableIntObjectInspector secondInt = PrimitiveObjectInspectorFactory.javaIntObjectInspector;

        try {
            udf.initialize(new ObjectInspector[] { firstInt, secondInt });
        } catch (UDFArgumentException e1) {
            e1.printStackTrace();
        }

        try {
            assertEquals(null, udf.evaluate(null, null));

            assertEquals(3, udf.evaluate(null, new DJO(3)));

            assertEquals(3, udf.evaluate(new DJO(3), null));

            assertEquals(12, udf.evaluate(new DJO(12), new DJO(5)));

            assertEquals(12, udf.evaluate(new DJO(3), new DJO(12), new DJO(5)));


        }   catch (HiveException e1) {
            e1.printStackTrace();
        }


    }

    /**
     * Compare Strings
     */
    @Test
    public void testEvaluateWithString() {
        GreatestUDF udf = new GreatestUDF();

        SettableStringObjectInspector secondString = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        SettableStringObjectInspector firstString = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        try {
            udf.initialize(new ObjectInspector[] { firstString, secondString });
        } catch (UDFArgumentException e1) {
            e1.printStackTrace();
        }

        try {

            assertEquals("oranges", udf.evaluate(new DJO("apples"), new DJO("oranges"), new DJO("bananas")));

            assertEquals("applis", udf.evaluate(new DJO("apples"), new DJO("applis"), new DJO("applas")));

        }   catch (HiveException e1) {
            e1.printStackTrace();
        }

    }
}
