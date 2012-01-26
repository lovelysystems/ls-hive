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
import org.apache.hadoop.io.Text;

@Description(name = "unescapexml",
        value = "_FUNC_(text) - Unescapes the basic xml entitities in text")
public final class UnescapeXMLUDF extends UDF {

    private final Text res = new Text();

    public static String unescapeXML(String txt) {
        if (txt.indexOf('&') >= 0) {
            return txt.replace("&lt;", "<").replace("&gt;", ">")
                    .replace("&apos;", "'").replace("&quot;", "\"")
                    .replace("&amp;", "&");
        }
        return txt;
    }

    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        } else if (s.find("&") == -1) {
            res.set(s);
        } else {
            res.set(s.toString());
        }
        return res;
    }

}