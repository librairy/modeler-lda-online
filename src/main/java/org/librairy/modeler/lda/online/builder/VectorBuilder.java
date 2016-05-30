package org.librairy.modeler.lda.online.builder;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.librairy.modeler.lda.online.data.BOW;
import org.librairy.modeler.lda.online.data.Vocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;

/**
 * Created on 17/05/16:
 *
 * @author cbadenes
 */
public class VectorBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(VectorBuilder.class);

    public static Vector from(BOW bow, Vocabulary vocabulary){
        LOG.info("Counting words to build the bag-of-words");

//        double[] bag = new double[vocabulary.getSize()];

        LinkedList<Integer> index   = new LinkedList<>();
        LinkedList<Double> tf       = new LinkedList<>();

        for(String word: bow.getFrequencies().keySet()){
            Long id = vocabulary.getWords().get(word);
            if (id == null) continue;// word not in vocabulary
//            bag[id.intValue()]=frequencies.get(word);
            index.add(id.intValue());
            tf.add(bow.getFrequencies().get(word).doubleValue());
        }

//        return Vectors.dense(bag);
        return Vectors.sparse(vocabulary.getWords().size(),
                ArrayUtils.toPrimitive(index.toArray(new Integer[index.size()])),
                ArrayUtils.toPrimitive(tf.toArray(new Double[tf.size()])));
    }

}
