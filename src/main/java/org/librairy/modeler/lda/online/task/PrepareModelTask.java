package org.librairy.modeler.lda.online.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationConfig;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.storage.StorageLevel;
import org.librairy.modeler.lda.online.builder.CorpusBuilder;
import org.librairy.modeler.lda.online.builder.ModelBuilder;
import org.librairy.modeler.lda.online.builder.SparkBuilder;
import org.librairy.modeler.lda.online.data.BOW;
import org.librairy.modeler.lda.online.data.SparseVector;
import org.librairy.modeler.lda.online.data.Vocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 09/05/16:
 *
 * @author cbadenes
 */
public class PrepareModelTask {

    private static final Logger LOG = LoggerFactory.getLogger(PrepareModelTask.class);

    public static final String VOCAB_FOLDER     = "/opt/spark/vocab/";
    public static final String BOW_FOLDER       = "/opt/spark/bow/";
    public static final String VECTOR_FOLDER    = "/opt/spark/vector/";

    private final CorpusBuilder corpusBuilder;
    private final ModelBuilder modelBuilder;
    private final SparkBuilder sparkBuilder;
    private final ObjectMapper jsonMapper;

    public PrepareModelTask(ApplicationContext ctx) throws IOException {
        this.corpusBuilder  = ctx.getBean(CorpusBuilder.class);
        this.modelBuilder   = ctx.getBean(ModelBuilder.class);
        this.sparkBuilder   = ctx.getBean(SparkBuilder.class);
        this.jsonMapper     = new ObjectMapper();

        Files.createDirectories(Paths.get(VOCAB_FOLDER));
        Files.createDirectories(Paths.get(BOW_FOLDER));
        Files.createDirectories(Paths.get(VECTOR_FOLDER));


    }


    public void run(String[] args) throws IOException {

        Integer size = 10;

        if (args.length >1) {
            size = Integer.valueOf(args[1]);
        }


        CassandraSQLContext cc = new CassandraSQLContext(sparkBuilder.sc.sc());

        DataFrame res = cc.sql("select uri,tokens from research.items");

        res.show();

        res.write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("hdfs://zavijava.dia.fi.upm.es/patents/tic/corpus.csv");

//        res.save("hdfs://zavijava.dia.fi.upm.es/patents/tic/corpus.csv", SaveMode.Overwrite);



//        // Create Bag-of-Words
//        List<Row> rows = res.select("uri").limit(size).collectAsList();
//        rows.parallelStream().forEach(row -> {
//
//            String uri      = row.getString(row.fieldIndex("uri"));
//            String name     = StringUtils.substringAfterLast(uri,"/");
//            File bowFile    = new File(BOW_FOLDER+name+".json");
//
//
//            if (!bowFile.exists()){
//                BOW bow = new BOW();
//                Map<String, Long> freq = res.select("uri","tokens").where("uri = '"+uri+"'")
//                        .toJavaRDD()
//                        .flatMap(r -> Arrays.asList(r.getString(r.fieldIndex("tokens")).split(" ")))
//                        .mapToPair(word -> new Tuple2<String, Long>(word, 1l))
//                        .reduceByKey((x, y) -> x + y)
//                        .collectAsMap();
//                bow.setFrequencies(freq);
//
//                try {
//                    jsonMapper.writeValue(bowFile,bow);
//                    LOG.info("BOW created for: " + uri + " at:" + bowFile.getAbsolutePath());
//                } catch (IOException e) {
//                    LOG.error("Error writing json: " + bowFile.getAbsolutePath(), e);
//                }
//            }else{
//                LOG.debug("BOW already exists for: " + uri);
//            }
//        });
//
//        // Create Vocabulary
//        Vocabulary vocabulary = new Vocabulary();
//
//        JavaPairRDD<String,Long> vocabularyRDD;
//        File vocabFile  = new File(VOCAB_FOLDER +"vocab-"+size+".json");
//        if (!vocabFile.exists()){
//            LOG.info("Creating vocabulary for " + size + " documents");
//            vocabularyRDD = res.select("tokens").limit(size)
//                    .toJavaRDD()
//                    .flatMap(row -> Arrays.asList(row.getString(row.fieldIndex("tokens")).split(" ")))
//                    .distinct()
//                    .zipWithIndex();
//
//            vocabulary.setWords(vocabularyRDD.collectAsMap());
//
//            jsonMapper.writeValue(vocabFile,vocabulary);
//            LOG.info("Vocabulary created at: " + vocabFile.getAbsolutePath());
//
//        }else{
//            LOG.info("Vocabulary already exists at: " + vocabFile.getAbsolutePath());
//            vocabulary = jsonMapper.readValue(vocabFile,Vocabulary.class);
//
//            LOG.info("Getting list of words as RDD: " + vocabFile.getAbsolutePath());
//            List<Tuple2<String, Long>> entryList = vocabulary.getWords().entrySet().parallelStream()
//                    .map(e -> new Tuple2<String, Long>(e.getKey(), e.getValue()))
//                    .collect(Collectors.toList());
//            vocabularyRDD = sparkBuilder.sc.parallelizePairs(entryList);
//        }
//        LOG.info("Vocabulary composed by " + vocabulary.getWords().size() + " words");

//        // Create Vector of frequencies
//        final Integer vocabularySize = vocabulary.getWords().size();
//        vocabularyRDD.persist(StorageLevel.MEMORY_AND_DISK());
//
//        rows.parallelStream().forEach(row -> {
//
//            String uri      = row.getString(row.fieldIndex("uri"));
//            String name     = StringUtils.substringAfterLast(uri,"/");
//            File bowFile    = new File(BOW_FOLDER+name+".json");
//            if (bowFile.exists()){
//                BOW bow = null;
//                try {
//
//                    LOG.info("deserializing bow json file");
//                    bow = jsonMapper.readValue(bowFile,BOW.class);
//
//                    LOG.info("converting to RDD");
//                    List<Tuple2<String,Long>> entryList = bow.getFrequencies().entrySet().stream()
//                            .map(e -> new Tuple2<String,Long>(e.getKey(), e.getValue()))
//                            .collect(Collectors.toList());
//                    JavaPairRDD<String,Long> bowRDD = sparkBuilder.sc.parallelizePairs(entryList);
//
//
//                    LOG.info("leftOuterJoin");
//                    Map<Integer,Double> vectorRDD = bowRDD.leftOuterJoin(vocabularyRDD)
//                            .map(l -> l._2)
//                            .filter(t -> t._2.isPresent())
//                            .mapToPair(t -> new Tuple2<Integer,Double>(t._2.get().intValue(), Double.valueOf(t._1)))
//                            .collectAsMap()
//                            ;
//
//                    LOG.info("creating sparse vector");
//                    SparseVector sparseVector = new SparseVector();
//                    sparseVector.setSize(vocabularySize);
//                    sparseVector.setElements(vectorRDD);
//
//                    LOG.info("serializing as json file");
//                    File vectorFile = new File(VECTOR_FOLDER +"vector-"+name+".json");
//                    jsonMapper.writeValue(vectorFile,sparseVector);
//                    LOG.info("(sparse) vector created for: " + uri + " at:"+ vectorFile.getAbsolutePath());
//
//                } catch (IOException e) {
//                    LOG.error("Error reading bow from: " + bowFile.getAbsolutePath());
//                }
//            }else{
//                LOG.debug("(sparse) vector already exists for: " + uri);
//            }
//
//            }
//        );

    }
}
