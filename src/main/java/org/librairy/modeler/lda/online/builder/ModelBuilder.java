package org.librairy.modeler.lda.online.builder;

import org.apache.spark.mllib.clustering.LDAModel;

/**
 * Created on 21/06/16:
 *
 * @author cbadenes
 */
public interface ModelBuilder {

    LDAModel train(Integer size, Integer vocabSize, Double alpha, Double beta, Integer topics, Integer iterations, Boolean perplexity);
}
