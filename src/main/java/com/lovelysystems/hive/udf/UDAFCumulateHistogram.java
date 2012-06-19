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

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import static com.lovelysystems.hive.udf.UDAFCollectHistogram.UDAFCollectHistogramEvaluator.mergeHistograms;


/**
 * Cumulate histogram over counts
 * 
 */
public final class UDAFCumulateHistogram extends UDAF {


    /**
     * The actual class for doing the aggregation. Hive will automatically look
     * for all internal classes of the UDAF that implements UDAFEvaluator.
     */
    public static class UDAFCumulateHistogramEvaluator implements UDAFEvaluator {

        UDAFCollectHistogram.UDAFCollectHistogramState state;

        public UDAFCumulateHistogramEvaluator() {
            super();
            state = new UDAFCollectHistogram.UDAFCollectHistogramState();
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
         * The number and type of arguments need to be the same as we call this
         * UDAF from Hive command line.
         *
         * This function should always return true.
         */
        public boolean iterate(HashMap<String, HashMap<Integer, ArrayList<Integer>>> o) {
            if (o == null) {
                return true;
            }
            state.elements = mergeHistograms(state.elements, o);
            return true;
        }


        /**
         * Terminate a partial aggregation and return the state. If the state is
         * a primitive, just return primitive Java classes like Integer or
         * String.
         */
        public UDAFCollectHistogram.UDAFCollectHistogramState terminatePartial() {
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

    private UDAFCumulateHistogram() {
        // prevent instantiation
    }
}
