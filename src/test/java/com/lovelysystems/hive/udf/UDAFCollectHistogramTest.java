package com.lovelysystems.hive.udf;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class UDAFCollectHistogramTest {

    /*
     * Collect counts of two rows.
     */
    @Test
    public void testCollectDifferentTS() {

        UDAFCollectHistogram.UDAFCollectHistogramEvaluator evaluator = new UDAFCollectHistogram.UDAFCollectHistogramEvaluator();

        evaluator.iterate("http://twitter.com/quodt", 1, "1 3 5");
        evaluator.iterate("http://twitter.com/quodt", 2, "2 5 9");

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> res = evaluator.terminate();

        assertEquals("{http://twitter.com/quodt={1=[1, 3, 5], 2=[2, 5, 9]}}", res.toString());

    }

    /**
     * The UDAF guarantees that data of a url and a particular timestamp is distinct but it does not guarantee which
     * dataset of ambiguous input data will be used for the result.
     */
    @Test
    public void testCollectSameTS() {

        UDAFCollectHistogram.UDAFCollectHistogramEvaluator evaluator = new UDAFCollectHistogram.UDAFCollectHistogramEvaluator();

        evaluator.iterate("http://twitter.com/quodt", 1, "1 3 5");
        evaluator.iterate("http://twitter.com/quodt", 1, "2 4 6");

        HashMap<String, HashMap<Integer, ArrayList<Integer>>> res = evaluator.terminate();

        assertEquals("{http://twitter.com/quodt={1=[1, 3, 5]}}", res.toString());

    }

}
