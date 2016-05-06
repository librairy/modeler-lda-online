package org.librairy.modeler.lda.online.builder;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.clustering.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.storage.StorageLevel;
import org.librairy.modeler.lda.online.data.Corpus;
import org.librairy.storage.UDM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 28/04/16:
 *
 * @author cbadenes
 */
@Component
public class ModelBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(ModelBuilder.class);

    @Autowired
    private SparkBuilder sparkBuilder;

    public LDAModel newModel(Corpus corpus, Double alpha, Double beta, Integer topics, Integer iterations){



        LOG.info("building a new onlineLDA model ..");






        Instant start = Instant.now();








        List<Tuple2<Long,Vector>> documents = corpus.getBagsOfWords()
                .entrySet()
                .parallelStream()
                .map(entry -> new Tuple2<>(entry.getKey(),entry.getValue()))
                .collect(Collectors.toList());

        JavaPairRDD<Long, Vector> bow = sparkBuilder.sc.parallelizePairs(documents);
        bow.cache();

//        JavaPairRDD<Long, Vector> bow = pairs.persist(StorageLevel.MEMORY_ONLY());

        Long startModel = System.currentTimeMillis();

        LOG.info
                ("====================================================================================================");
        LOG.info(" TRAINING-STAGE: alpha=" + alpha + ", beta=" + beta + ", numTopics=" + topics + ", " +
                "numIterations="+iterations+", corpusSize="  + corpus.getDocuments().size());
        LOG.info
                ("====================================================================================================");

//        JavaPairRDD<Long, Vector> trainingBagsOfWords = corpus.getBagsOfWords();
//        trainingBagsOfWords.persist(StorageLevel.MEMORY_AND_DISK()); //MEMORY_ONLY_SER

//        Broadcast<JavaPairRDD<Long, Vector>> trainingBOW = sparkBuilder.sc.broadcast(bow);
//        LOG.info("Size of training-bow: " + SizeEstimator.estimate(trainingBOW.getValue()));

        // Online LDA Model :: Creation
        // -> Online Optimizer
//        Double TAU              =   1.0;  // how downweight early iterations
//        Double KAPPA            =   0.5;  // how quickly old information is forgotten
//        //Double BATCH_SIZE_RATIO  =   Math.min(1.0,2.0 / iterations + 1.0 / corpus.getDocuments().size());  // how many
//        // documents
//        Double BATCH_SIZE_RATIO  =   0.3;
//        OnlineLDAOptimizer onlineLDAOptimizer = new OnlineLDAOptimizer()
//                .setMiniBatchFraction(BATCH_SIZE_RATIO)
//                .setOptimizeDocConcentration(true)
//                .setTau0(TAU)
//                .setKappa(KAPPA)
//                ;
//
//        LOG.info("Building the model...");
//        LDAModel ldaModel = new LDA().
//                setAlpha(alpha).
//                setBeta(beta).
//                setK(topics).
//                setMaxIterations(iterations).
//                setOptimizer(onlineLDAOptimizer).
//                run(bow);
//
//        LocalLDAModel ldaModelWrapper = (LocalLDAModel) ldaModel;

        LDAModel ldaModel = new LDA()
                .setK(topics)
                .run(bow);

        LOG.info("## Created Distributed LDA Model successfully!!!!");
        DistributedLDAModel distributedLDAModel = (DistributedLDAModel) ldaModel;

        Instant end = Instant.now();
        LOG.info("Elapsed Time: "       + ChronoUnit.MINUTES.between(start,end) + "min " + (ChronoUnit.SECONDS
                .between(start,end)%60) + "secs");

        LOG.info("## Creating Local LDA Model ..");
        LocalLDAModel localModel = distributedLDAModel.toLocal();
        LOG.info("## Online LDA Model :: Description");
        LOG.info("Log-Likelihood: "     + distributedLDAModel.logLikelihood());
//        LOG.info("Log-Perplexity: "     + localModel.logPerplexity(bow));
        LOG.info("Vocabulary Size: "    + localModel.vocabSize());
        return localModel;
    }
}
