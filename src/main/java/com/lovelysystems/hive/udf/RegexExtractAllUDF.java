package com.lovelysystems.hive.udf;

import java.util.LinkedList;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDF to extract a specific group identified by a java regex. Note that if a
 * regexp has a backslash ('\'), then need to specify '\\' For example,
 * regexp_extract('100-200', '(\\d+)-(\\d+)', 1) will return '100'
 */
@Description(
        name = "regexpextractall",
        value = "_FUNC_(haystack, pattern, [index]) - Find all the instances of pattern in haystack.")
public class RegexExtractAllUDF extends UDF {
    private String lastRegex = null;
    private Pattern p = null;

    public LinkedList<String> evaluate(String s, String regex,
            Integer extractIndex) {
        if (s == null || regex == null) {
            return null;
        }
        if (!regex.equals(lastRegex)) {
            lastRegex = regex;
            p = Pattern.compile(regex, Pattern.MULTILINE);
        }
        LinkedList<String> result = new LinkedList<String>();
        Matcher m = p.matcher(s);
        while (m.find()) {
            MatchResult mr = m.toMatchResult();
            result.add(mr.group(extractIndex));
        }
        return result;
    }

    public LinkedList<String> evaluate(String s, String regex) {
        return this.evaluate(s, regex, 1);
    }
}
