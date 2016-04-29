package org.librairy.modeler.lda.online.builder;

import com.google.common.base.CharMatcher;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 28/04/16:
 *
 * @author cbadenes
 */
public class BowBuilder {

    private static final List<String> STOPWORDS = Arrays.asList(new String[]{
            "fig.","sample","figs."
    });


    public static Vector from(List<String> tokens, Map<String,Long> vocabulary){
        return from(count(tokens),vocabulary);
    }

    public static Vector from(Map<String,Long> frequencies, Map<String,Long> vocabulary){
        double[] bag = new double[vocabulary.size()];
        for(String word: frequencies.keySet()){
            Long id = vocabulary.get(word);
            if (id == null) continue;// word not in vocabulary
            bag[id.intValue()]=frequencies.get(word);
        }

        return Vectors.dense(bag);
    }


    public static Map<String,Long> count(List<String> tokens){
        return tokens.stream().
                filter(token -> isValid(token))
                .collect(Collectors.groupingBy(token -> token, Collectors.counting()));
    }

    private static boolean isValid(String token){
        return !STOPWORDS.contains(token.toLowerCase()) &&
                CharMatcher.ASCII.matchesAllOf(token) &&
                CharMatcher.JAVA_LETTER.countIn(token) > 1
                ;
    }

}
