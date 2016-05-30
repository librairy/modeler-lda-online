package org.librairy.modeler.lda.online;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.cbadenes.lab.test.IntegrationTest;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.librairy.Application;
import org.librairy.modeler.lda.online.builder.BowBuilder;
import org.librairy.modeler.lda.online.builder.SparkBuilder;
import org.librairy.modeler.lda.online.data.Vocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created on 09/05/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
public class VocabularyTest {

    private static final Logger LOG = LoggerFactory.getLogger(VocabularyTest.class);

    @Test
    public void loadVocabulary() throws IOException {


        ObjectMapper jsonMapper = new ObjectMapper();
        File vocabFile = new File("/opt/spark/inbox/vocabulary-all.json");

        Vocabulary vocabulary = jsonMapper.readValue(vocabFile, Vocabulary.class);

        System.out.println(vocabulary.getWords().size());
        System.out.println(vocabulary.getWords().get("copier"));

    }


    @Test
    public void createVocabulary() throws IOException {


        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "wiig.dia.fi.upm.es")
//                .set("spark.cassandra.connection.host", "192.168.99.100")
                .set("spark.cassandra.connection.port", "5011")
                .set("spark.cassandra.input.split.size_in_mb","1280")
//                .set("spark.cassandra.input.fetch.size_in_rows","1000")
//                .set("spark.cassandra.input.split.size","10000")
//                .set("spark.cassandra.output.throughput_mb_per_sec","10")
                ;

        JavaSparkContext sc = new JavaSparkContext("local", "cassandra-test", conf);

        CassandraSQLContext cc = new CassandraSQLContext(sc.sc());

        Instant s1 = Instant.now();

        String sql = "select uri,tokens from research.items";

        LOG.info("Getting texts from Cassandra by '" + sql + "'");

        DataFrame items = cc.sql(sql);

        JavaRDD<Tuple2<String, List<String>>> tokens = items.javaRDD().map(row -> new Tuple2<String, List<String>>(row.getString
                (row.fieldIndex("uri")), Arrays.asList(row.getString(row.fieldIndex("tokens")).split(" "))));

//        JavaRDD<Tuple2<String, Map<String, Long>>> bows = tokens.map(tuple -> new Tuple2<String, Map<String, Long>>
//                (tuple._1, BowBuilder.count(tuple._2())));

        LOG.info("Composing the Vocabulary...");

        JavaPairRDD<String, Long> words = tokens
                .flatMap(resource -> resource._2)
                .distinct()
                .zipWithIndex();

        LOG.info("Trying to collect as map...");

        Map<String, Long> modelVocabulary = words.collectAsMap();

        LOG.info("Preparing vocabulary...");

        Vocabulary vocabulary = new Vocabulary();
        vocabulary.setWords(modelVocabulary);

        LOG.info( modelVocabulary.size() + " words" );
        Instant e1 = Instant.now();
        LOG.info("Vocabulary composed in: "       + ChronoUnit.MINUTES.between(s1,e1) + "min " +
                (ChronoUnit.SECONDS.between(s1,e1)%60) + "secs");

        LOG.info("Serializing vocabulary as json-file .. ");
        ObjectMapper jsonMapper = new ObjectMapper();
        File vocabFile = new File("/opt/spark/inbox/vocabulary-all.json");

        jsonMapper.writeValue(vocabFile,vocabulary);

        LOG.info("Vocabulary saved!!!");


    }

}
