package com.lovelysystems.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;


public class ArraySumUDFTest {
    @Test
    public void testEvaluate() {
        ArraySumUDF udf = new ArraySumUDF();

        ArrayList<ArrayList<Integer>> args = new ArrayList<ArrayList<Integer>>();

        assertEquals(new org.apache.hadoop.io.IntWritable(0), udf.evaluate(args));

        ArrayList<Integer> item_1 = new ArrayList<Integer>();
        item_1.add(1);
        item_1.add(2);
        item_1.add(3);
        args.add(item_1);

        ArrayList<Integer> item_2 = new ArrayList<Integer>();
        item_2.add(4);
        item_2.add(5);
        args.add(item_2);

        ArrayList<Integer> item_3 = new ArrayList<Integer>();
        args.add(item_3);

        ArrayList<Integer> item_4 = new ArrayList<Integer>();
        item_4.add(null);
        args.add(item_4);

        assertEquals(new org.apache.hadoop.io.IntWritable(15), udf.evaluate(args));

    }
}
