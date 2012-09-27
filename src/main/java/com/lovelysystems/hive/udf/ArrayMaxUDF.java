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
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;

@UDFType(deterministic = true)
@Description(name = "arraysum",
        value = "_FUNC_(a) - Returns the sum of array items")
public class ArrayMaxUDF extends UDF {

    private final IntWritable result = new IntWritable();

    public IntWritable evaluate(ArrayList<ArrayList<Integer>> a) {
        if (a == null) {
            return null;
        }

        int max = 0;
        for (ArrayList<Integer> aa : a) {
            if (aa != null) {
                for (Integer i : aa) {
                    if (i != null) {
                        if (i > max) {
                            max = i;
                        }
                    }
                }
            }
        }
        result.set(max);
        return result;
    }
}