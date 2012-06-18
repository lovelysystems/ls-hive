package com.lovelysystems.hive.udf;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class UDAFCollectHistogramTest {

    /*
     * cumulate two rows
     */
    @Test
    public void testCollect() {

        UDAFCollectHistogram.UDAFCollectHistogramEvaluator evaluator = new UDAFCollectHistogram.UDAFCollectHistogramEvaluator();

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> row1 = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
        HashMap<Integer, ArrayList<Integer>> counts1 = new HashMap<Integer, ArrayList<Integer>>();
        counts1.put(1, new ArrayList<Integer>(Arrays.asList(1, 3, 5)));
        row1.put("http://twitter.com/quodt", counts1);

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> row2 = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
        HashMap<Integer, ArrayList<Integer>> counts2 = new HashMap<Integer, ArrayList<Integer>>();
        counts2.put(2, new ArrayList<Integer>(Arrays.asList(2, 5, 9)));
        row2.put("http://twitter.com/quodt", counts2);

        evaluator.iterate("http://twitter.com/quodt", 1, "1 3 5");
        evaluator.iterate("http://twitter.com/quodt", 2, "2 5 9");

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> res = evaluator.terminate();

        assertEquals("{http://twitter.com/quodt={1=[1, 3, 5], 2=[2, 5, 9]}}", res.toString());

    }

}
