package org.librairy.modeler.lda.online.builder;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.librairy.modeler.lda.online.functions.RowToPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Created on 28/04/16:
 *
 * @author cbadenes
 */
@Component
public class OnlineModelBuilder implements ModelBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(OnlineModelBuilder.class);

    @Autowired
    private SparkBuilder sparkBuilder;

    public LDAModel train(Integer size, Integer vocabSize, Double alpha, Double beta, Integer topics, Integer
            iterations, Boolean perplexity){

        LOG.info("building a new LDA model by using the OnlineLDA optimizer..");

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

        int processors = Runtime.getRuntime().availableProcessors()*2; //2 or 3 times

        int numPartitions = Math.max(processors, size/processors);

        LOG.info("Num Partitions set to: " + numPartitions);

        DataFrame df = patentsDF
                .limit(size)
                .repartition(numPartitions)
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
//                .setMinDF(5)    // Specifies the minimum number of different documents a term must appear in to be included in the vocabulary.
//                .setMinTF(50)   // Specifies the minimumn number of times a term has to appear in a document to be
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

        // Bayesian Optimizer
        LDAModel ldaModel = lda.run(documents);
        LocalLDAModel localModel = (LocalLDAModel) ldaModel;

        Instant endModel = Instant.now();
        Instant end = Instant.now();
        LOG.info("## LDA Model created successfully!!!!");



        LOG.info("#####################################################################################");
        LOG.info("Model Elapsed Time: "       + ChronoUnit.MINUTES.between(startModel,endModel) + "min " + (ChronoUnit
                .SECONDS
                .between(startModel,endModel)%60) + "secs");
        LOG.info("Total Elapsed Time: "       + ChronoUnit.MINUTES.between(start,end) + "min " + (ChronoUnit.SECONDS
                .between(start,end)%60) + "secs");
        LOG.info("Vocabulary Size: "    + vocabSize + "/" + ldaModel.vocabSize());
        LOG.info("Corpus Size: "    + size + "/" + documents.count());
        LOG.info("Num Topics: "    + topics);
        LOG.info("Num Iterations: "    + iterations);
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
