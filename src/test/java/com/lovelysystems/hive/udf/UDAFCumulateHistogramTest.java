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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class UDAFCumulateHistogramTest {

    /*
     * cumulate two rows with different timestamp
     */
    @Test
    public void testCumulateDifferentTS() {

        UDAFCumulateHistogram.UDAFCumulateHistogramEvaluator evaluator = new UDAFCumulateHistogram.UDAFCumulateHistogramEvaluator();

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> row1 = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
        HashMap<Integer, ArrayList<Integer>> counts1 = new HashMap<Integer, ArrayList<Integer>>();
        counts1.put(1, new ArrayList<Integer>(Arrays.asList(1, 3, 5)));
        row1.put("http://twitter.com/quodt", counts1);

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> row2 = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
        HashMap<Integer, ArrayList<Integer>> counts2 = new HashMap<Integer, ArrayList<Integer>>();
        counts2.put(2, new ArrayList<Integer>(Arrays.asList(2, 5, 9)));
        row2.put("http://twitter.com/quodt", counts2);

        evaluator.iterate(row1);
        evaluator.iterate(row2);

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> res = evaluator.terminate();

        assertEquals("{http://twitter.com/quodt={1=[1, 3, 5], 2=[2, 5, 9]}}", res.toString());

    }

    /*
     * cumulate two rows with counts with same timestamp
     */
    @Test
    public void testCumulateSameTS() {

        UDAFCumulateHistogram.UDAFCumulateHistogramEvaluator evaluator = new UDAFCumulateHistogram.UDAFCumulateHistogramEvaluator();

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> row1 = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
        HashMap<Integer, ArrayList<Integer>> counts1 = new HashMap<Integer, ArrayList<Integer>>();
        counts1.put(1, new ArrayList<Integer>(Arrays.asList(1, 3, 5)));
        row1.put("http://twitter.com/quodt", counts1);

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> row2 = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
        HashMap<Integer, ArrayList<Integer>> counts2 = new HashMap<Integer, ArrayList<Integer>>();
        counts2.put(1, new ArrayList<Integer>(Arrays.asList(2, 4, 6)));
        row2.put("http://twitter.com/quodt", counts2);

        evaluator.iterate(row1);
        evaluator.iterate(row2);

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> res = evaluator.terminate();

        assertEquals("{http://twitter.com/quodt={1=[2, 4, 6]}}", res.toString());

    }

}
