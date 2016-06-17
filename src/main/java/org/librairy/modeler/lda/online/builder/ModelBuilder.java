package org.librairy.modeler.lda.online.builder;

import lombok.val;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.clustering.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.librairy.modeler.lda.online.data.Corpus;
import org.librairy.modeler.lda.online.functions.RowToPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Function1;
import scala.Function1$class;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

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

    public LDAModel train(Corpus corpus, Double alpha, Double beta, Integer topics, Integer iterations, Boolean
            perplexity){



        LOG.info("building a new onlineLDA model ..");

        Instant start = Instant.now();

        JavaPairRDD<Long, Vector> bow = corpus.getBagsOfWords();

//        JavaPairRDD<Long, Vector> bow = pairs.persist(StorageLevel.MEMORY_ONLY());

        LOG.info
                ("====================================================================================================");
        LOG.info(" TRAINING-STAGE: alpha=" + alpha + ", beta=" + beta + ", numTopics=" + topics + ", " +
                "numIterations="+iterations+", corpusSize="  + corpus.getSize() +", vocabulary=" + corpus.getVocabularySize()
                + "words");
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


        // Setting Checkpoint

//        lda.setOptimizer(optimizer)
//                .setK(params.k)
//                .setMaxIterations(params.maxIterations)
//                .setDocConcentration(params.docConcentration)
//                .setTopicConcentration(params.topicConcentration)
//                .setCheckpointInterval(params.checkpointInterval)
//        if (params.checkpointDir.nonEmpty) {
//            sc.setCheckpointDir(params.checkpointDir.get)
//        }

        //optimizer = new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)


        LDA lda = new LDA()
                .setOptimizer(new OnlineLDAOptimizer())
                .setK(topics)
                .setMaxIterations(iterations)
                .setDocConcentration(alpha)
                .setTopicConcentration(beta)
                .setCheckpointInterval(10)
                ;

        LDAModel ldaModel = lda.run(bow);

        LOG.info("## Created Distributed LDA Model successfully!!!!");
//        DistributedLDAModel distributedLDAModel = (DistributedLDAModel) ldaModel;

        Instant end = Instant.now();
        LOG.info("Elapsed Time: "       + ChronoUnit.MINUTES.between(start,end) + "min " + (ChronoUnit.SECONDS
                .between(start,end)%60) + "secs");

        LOG.info("## Creating Local LDA Model ..");
//        LocalLDAModel localModel = distributedLDAModel.toLocal();
        LocalLDAModel localModel = (LocalLDAModel) ldaModel;
        LOG.info("## Online LDA Model :: Description");
        LOG.info("Vocabulary Size: "    + localModel.vocabSize());
//        LOG.info("Log-Likelihood: "     + distributedLDAModel.logLikelihood());
        if (perplexity){
            Instant startPartial = Instant.now();
            LOG.info("Log-Perplexity: "     + localModel.logPerplexity(bow));
            Instant endPartial = Instant.now();
            LOG.info("Perplexity elapsed Time: "       + ChronoUnit.MINUTES.between(startPartial,endPartial) + "min " +
                    (ChronoUnit.SECONDS
                    .between(startPartial,endPartial)%60) + "secs");
        }
        return ldaModel;
    }


    public LDAModel train(Integer size, Integer vocabSize, Double alpha, Double beta, Integer topics, Integer
            iterations, Boolean
            perplexity){

        LOG.info("building a new LDA model ..");

        Instant start = Instant.now();

        LOG.info
                ("====================================================================================================");
        LOG.info(" TRAINING-STAGE: alpha=" + alpha + ", beta=" + beta + ", numTopics=" + topics + ", " +
                "numIterations="+iterations+", corpusSize="  + size +", vocabulary=" + vocabSize);
        LOG.info
                ("====================================================================================================");

        /**********************************************************************
         * Simulated DataFrame
         **********************************************************************/
//        JavaRDD<Row> jrdd = sparkBuilder.sc.parallelize(Arrays.asList(
//                RowFactory.create(0, "Hi I heard about Spark"),
//                RowFactory.create(1, "I wish Java could use case classes"),
//                RowFactory.create(2, "Logistic,regression,models,are,neat")
//        ));
//
//        StructType schema = new StructType(new StructField[]{
//                new StructField("uri", DataTypes.IntegerType, false, Metadata.empty()),
//                new StructField("tokens", DataTypes.StringType, false, Metadata.empty())
//        });
//
//        SQLContext sqlContext = new SQLContext(sparkBuilder.sc);
//
//        DataFrame df = sqlContext.createDataFrame(jrdd, schema);


        /**********************************************************************
         * Cassandra DataFrame
         **********************************************************************/
//        CassandraSQLContext cc = new CassandraSQLContext(sparkBuilder.sc.sc());
//
//        String sql = "select uri,tokens from research.items";
//
//        LOG.info("Getting texts from Cassandra by '" + sql + "'");
//        DataFrame patentsDF = cc.cassandraSql(sql);

        /**********************************************************************
         * CSV DataFrame
         **********************************************************************/
        SQLContext sqlContext = new SQLContext(sparkBuilder.sc);
        DataFrame patentsDF = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true") // Use first line of all files as header
                .option("inferSchema", "true") // Automatically infer data types
                .load("hdfs://zavijava.dia.fi.upm.es/patents/tic/corpus.csv");


        /**********************************************************************
         * Topic Modeling
         **********************************************************************/

        DataFrame df = patentsDF
                .limit(size)
                .repartition(10000) //TODO To be calculated from corpus size
                ;

        LOG.info("Splitting each document into words ..");
        DataFrame words = new Tokenizer()
                .setInputCol("tokens")
                .setOutputCol("words")
                //.setMinTokenLength(4) // Filter away tokens with length < 4
                .transform(df);

        LOG.info("Filter out stopwords");
        String[] stopwords = new String[]{"fig."};
        DataFrame filteredWords = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered")
                .setStopWords(stopwords)
                .setCaseSensitive(false)
                .transform(words);

        LOG.info("Limiting to top `vocabSize` most common words and convert to word count vector features ..");
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("filtered")
                .setOutputCol("features")
                .setVocabSize(vocabSize)
                .setMinDF(5)    // Specifies the minimum number of different documents a term must appear in to be included in the vocabulary.
                .setMinTF(50)   // Specifies the minimumn number of times a term has to appear in a document to be
                // included in the vocabulary.
                .fit(filteredWords);

        Tuple2<Object, Vector> tuple = new Tuple2<Object,Vector>(0l, Vectors.dense(new double[]{1.0}));

        RDD<Tuple2<Object, Vector>> documents = cvModel.transform(filteredWords).select("uri",
                "features").map(new RowToPair(), ClassTag$.MODULE$.<Tuple2<Object, Vector>>apply(tuple.getClass()))
                .cache();


                // Spark-MLLIB Features:
//        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{tokenizer,filterer,cVectorizer});
//        PipelineModel model = pipeline.fit(df);


                LOG.info("Building a corpus by using bag-of-words ..");

//        JavaPairRDD<Long, Vector> documents = cvModel.transform(filteredWords)
//                .select("features")
//                .toJavaRDD()
//                .map(row -> (Vector) row.get(0))
//                .zipWithIndex()
//                .mapToPair(r -> r.swap())
////                .repartition(32) // 32  //500
//                .cache();
//        ;



        LOG.info("Configuring LDA ..");
        double mbf = 2.0 / iterations + 1.0 / size;

        LDA lda = new LDA()
                .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
                .setK(topics)
                .setMaxIterations(iterations)
                .setDocConcentration(alpha)
                .setTopicConcentration(beta)
//                .setCheckpointInterval(2)
                ;

        LOG.info("Running LDA on corpus ..");
        Instant startModel = Instant.now();
        LDAModel ldaModel = lda.run(documents);
        Instant endModel = Instant.now();

        Instant end = Instant.now();
        LocalLDAModel localModel = (LocalLDAModel) ldaModel;
        LOG.info("## Created LDA Model successfully!!!!");
        LOG.info("#####################################################################################");
        LOG.info("Model Elapsed Time: "       + ChronoUnit.MINUTES.between(startModel,endModel) + "min " + (ChronoUnit
                .SECONDS
                .between(startModel,endModel)%60) + "secs");
        LOG.info("Total Elapsed Time: "       + ChronoUnit.MINUTES.between(start,end) + "min " + (ChronoUnit.SECONDS
                .between(start,end)%60) + "secs");
        LOG.info("Vocabulary Size: "    + localModel.vocabSize());
        LOG.info("Corpus Size: "    + size);
        LOG.info("Num Iterations: "    + iterations);
        LOG.info("Num Topics: "    + topics);
        LOG.info("Alpha: "    + alpha);
        LOG.info("Beta: "    + beta);
        LOG.info("#####################################################################################");

        Tuple2<int[], double[]>[] topicIndices = ldaModel.describeTopics(10);
        String[] vocabArray = cvModel.vocabulary();

        int index = 0;
        for (Tuple2<int[], double[]> topic : topicIndices){
            LOG.info("Topic-" + index++);
            int[] topicWords = topic._1;
            double[] weights = topic._2;

            for (int i=0; i< topicWords.length;i++){
                int wid = topicWords[i];
                LOG.info("\t"+vocabArray[wid] +"\t:" + weights[i]);
            }
            LOG.info("------------------------------------------");
        }

        return ldaModel;


    }
}
