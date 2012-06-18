/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lovelysystems.hive.udf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Compute (normalized) entropy over a series of values.
 * 
 */
public final class UDAFCollectHistogram extends UDAF {

    /**
     * Note that this is only needed if the internal state cannot be represented
     * by a primitive.
     * 
     * The internal state can also contains fields with types like
     * ArrayList<String> and HashMap<String,Double> if needed.
     */
    public static class UDAFCollectHistogramState {
        public HashMap<String, HashMap<Integer, ArrayList<Integer>>> elements;
    }

    /**
     * The actual class for doing the aggregation. Hive will automatically look
     * for all internal classes of the UDAF that implements UDAFEvaluator.
     */
    public static class UDAFCollectHistogramEvaluator implements UDAFEvaluator {

        UDAFCollectHistogramState state;

        public UDAFCollectHistogramEvaluator() {
            super();
            state = new UDAFCollectHistogramState();
            init();
        }

        /**
         * Reset the state of the aggregation.
         */
        public void init() {
            state.elements = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
        }

        /**
         * Iterate through one row of original data.
         * 
         * The number and type of arguments need to the same as we call this
         * UDAF from Hive command line.
         * 
         * This function should always return true.
         */
        public boolean iterate(String k, Integer ts, String values) {
            if (k != null && ts != null && values != null) {
                HashMap<Integer, ArrayList<Integer>> histo = state.elements.get(k);
                if (histo != null){
                    if (histo.containsKey(ts)){
                        return true;
                    }
                }
                String[] sc = values.trim().split(" ");
                if (sc.length == 0) {
                    return true;
                }
                ArrayList<Integer> ic = new ArrayList<Integer>(sc.length);
                for (String s : sc) {
                    try {
                        ic.add(Integer.parseInt(s));
                    } catch (NumberFormatException e) {
                        return true;
                    }
                }
                if (histo == null){
                    histo = new HashMap<Integer, ArrayList<Integer>>();
                }
                histo.put(ts, ic);
                state.elements.put(k, histo);
            }
            return true;
        }

        /**
         * Terminate a partial aggregation and return the state. If the state is
         * a primitive, just return primitive Java classes like Integer or
         * String.
         */
        public UDAFCollectHistogramState terminatePartial() {
            // Return null if we have no data.
            if (state.elements.size() == 0) {
                return null;
            } else {
                return state;
            }
        }



        /**
         * Terminates the aggregation and return the final result.
         */
        public HashMap<String, HashMap<Integer, ArrayList<Integer>>> terminate() {
            return state.elements;
        }
    }

    private UDAFCollectHistogram() {
        // prevent instantiation
    }
}
