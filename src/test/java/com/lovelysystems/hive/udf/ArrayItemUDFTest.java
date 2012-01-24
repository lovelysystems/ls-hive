package com.lovelysystems.hive.udf;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.junit.Test;

public class ArrayItemUDFTest {

    class DeferredJavaObject implements DeferredObject {

        private Object v;

        public DeferredJavaObject(Object v) {
            this.v = v;
        }

        @Override
        public Object get() throws HiveException {
            return v;
        }

    }

    @Test
    public void testEvaluate() {
        ArrayItemUDF udf = new ArrayItemUDF();

        SettableListObjectInspector arrayOI = ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        SettableIntObjectInspector posOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        try {
            udf.initialize(new ObjectInspector[] { arrayOI, posOI });
        } catch (UDFArgumentException e1) {
            e1.printStackTrace();
        }

        Object pos = posOI.create(1);
        Object arr = arrayOI.create(3);
        arrayOI.set(arr, 0, "first");
        arrayOI.set(arr, 1, "second");
        arrayOI.set(arr, 2, "third");

        DeferredObject[] args = new DeferredJavaObject[] {
                new DeferredJavaObject(arr), new DeferredJavaObject(pos) };

        try {
            assertEquals("second", udf.evaluate(args));
        } catch (HiveException e) {
            e.printStackTrace();
        }
        args[1] = new DeferredJavaObject(posOI.create(-1));
        try {
            assertEquals("third", udf.evaluate(args));
        } catch (HiveException e) {
            e.printStackTrace();
        }
        arr = arrayOI.create(1);
        arrayOI.set(arr, 0, "theonly");
        args[0] = new DeferredJavaObject(arr);
        try {
            assertEquals("theonly", udf.evaluate(args));
        } catch (HiveException e) {
            e.printStackTrace();
        }
        arr = arrayOI.create(0);
        args[0] = new DeferredJavaObject(arr);
        try {
            assertEquals(null, udf.evaluate(args));
        } catch (HiveException e) {
            e.printStackTrace();
        }

    }
}
