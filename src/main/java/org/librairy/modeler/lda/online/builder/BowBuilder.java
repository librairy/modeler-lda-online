package org.librairy.modeler.lda.online.builder;

import com.google.common.base.CharMatcher;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.librairy.modeler.lda.online.data.BOW;
import org.librairy.modeler.lda.online.data.Vocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 28/04/16:
 *
 * @author cbadenes
 */
public class BowBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(BowBuilder.class);

    private static final List<String> STOPWORDS = Arrays.asList(new String[]{
            "fig.",
            "sample",
            "figs."
    });

    public static BOW from(List<String> tokens){
        BOW bow = new BOW();
        bow.setFrequencies(tokens.stream().
                filter(token -> isValid(token))
                .collect(Collectors.groupingBy(token -> token, Collectors.counting())));
        return bow;
    }

    private static boolean isValid(String token){
        return !STOPWORDS.contains(token.toLowerCase()) &&
                CharMatcher.ASCII.matchesAllOf(token) &&
                CharMatcher.JAVA_LETTER.countIn(token) > 1
                ;
    }

}
