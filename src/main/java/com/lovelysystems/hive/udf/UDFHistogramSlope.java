/**
 * Copyright 2012 Lovely Systems GmbH
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.lovelysystems.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

@UDFType(deterministic = true)
@Description(name = "histogram_slope",
        value = "_FUNC_(histo) - Returns the slope of the histogram")
public class UDFHistogramSlope extends UDF {

    @SuppressWarnings("unchecked")
    public Float evaluate(
            HashMap<Integer, ArrayList<Integer>> histo
    ) {
        if (histo == null || histo.size() < 2) {
            return null;
        }

        Integer lastTS = Integer.MIN_VALUE;
        Integer firstTS = Integer.MAX_VALUE;

        Integer firstValue = null;
        Integer lastValue = null;


        for (Entry<Integer, ArrayList<Integer>> re : histo
                .entrySet()) {

            int count = 0;
            for (int i : re.getValue()) count += i;
            if (count == 0) {
                continue;
            }


            int ts = re.getKey();

            if (ts < firstTS) {
                if (firstValue != null && lastValue == null) {
                    lastValue = firstValue;
                    lastTS = firstTS;
                }
                firstTS = ts;
                firstValue = count;
            } else if (ts > lastTS) {
                if (lastValue != null && firstValue == null) {
                    firstValue = lastValue;
                    firstTS = lastTS;
                }
                lastTS = ts;
                lastValue = count;
            }
        }

        if ((firstValue == null) || (lastValue == null)) {
            return null;
        }

        Integer deltaV = lastValue - firstValue;
        Integer deltaT = lastTS - firstTS;

        if ((deltaV < 0) || (deltaT <= 0)) {
            return null;
        }

        return deltaV.floatValue() / deltaT.floatValue();


    }

}
