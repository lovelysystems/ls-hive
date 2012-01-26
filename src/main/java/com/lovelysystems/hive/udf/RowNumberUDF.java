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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Number rows.
 */
@Description(
        name = "NUMBER_ROWS",
        value = "_FUNC_(key1, key2, ...) - Number rows starting at 1.  Whenever the value of any key changes the numbering is reset to 1.")
public class RowNumberUDF extends UDF {
    Object[] previous_keys = null;
    int previous_index;

    public int evaluate(Object... keys) {
        if (previous_keys == null || !Arrays.equals(previous_keys, keys)) {
            previous_index = 0;
            previous_keys = keys.clone();
        }
        previous_index++;
        return previous_index;
    }
}
