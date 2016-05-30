package org.librairy.modeler.lda.online.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import es.cbadenes.lab.test.IntegrationTest;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.librairy.modeler.lda.online.data.BOW;
import org.librairy.modeler.lda.online.data.SparseVector;
import org.librairy.modeler.lda.online.data.Vocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 17/05/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
public class VectorTest {

    private static final Logger LOG = LoggerFactory.getLogger(VectorTest.class);

    @Test
    public void merge() throws IOException {

        Vocabulary vocabulary = new Vocabulary();
        vocabulary.setWords(ImmutableMap.of("a",0l,"b",1l,"c",2l,"d",3l));
        LOG.info("Vocabulary: " + vocabulary);

        BOW bow = new BOW();
        bow.setFrequencies(ImmutableMap.of("e",2l,"b",5l,"h",1l,"d",1l));
        LOG.info("BOW: " + bow);


        SparkConf sconf = new SparkConf()
                .setAppName("librairy.lda.modeler")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sconf);

        JavaPairRDD<String,Long> vocabularyRDD = sc.parallelizePairs(vocabulary.getWords().entrySet()
                .stream().map(t -> new Tuple2<String,Long>(t.getKey(), t.getValue())).collect(Collectors.toList()));

        JavaPairRDD<String,Long> bowRDD = sc.parallelizePairs(bow.getFrequencies().entrySet().stream().map(t -> new
                Tuple2<String,Long>(t.getKey(), t.getValue())).collect(Collectors.toList()));


//        JavaRDD<Tuple2<Integer,Double>> vectorRDD = vocabularyRDD.leftOuterJoin(bowRDD).map(l -> l._2)
//                .filter(t -> t._2.isPresent()).map(t -> new Tuple2(t._1.intValue(), Double.valueOf(t._2.get())));

        JavaPairRDD<Integer,Double> vectorRDD = bowRDD.leftOuterJoin(vocabularyRDD).map(l -> l._2)
                .filter(t -> t._2.isPresent()).mapToPair(t -> new Tuple2(t._2.get().intValue(), Double.valueOf(t._1)));


        SparseVector sparseVector = new SparseVector();
        sparseVector.setSize(vocabulary.getWords().size());
        sparseVector.setElements(vectorRDD.collectAsMap());

        Vector vector = Vectors.sparse(vocabulary.getWords().size(), vectorRDD.collect());
        LOG.info("Sparse vector: " + vector);

        ObjectMapper jsonMapper = new ObjectMapper();

        File vectorFile = new File("/opt/spark/vectors/test.json");
        jsonMapper.writeValue(vectorFile,sparseVector);

        LOG.info("Wrote json file: " + vectorFile.getAbsolutePath());

    }

    @Test
    public void load() throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();

        File vectorFile = new File("/opt/spark/vectors/test.json");
        SparseVector sparseVector = jsonMapper.readValue(vectorFile, SparseVector.class);
        LOG.info("Vector loaded: " + sparseVector);

        Vector vector = Vectors.sparse(
                sparseVector.getSize(),
                ArrayUtils.toPrimitive(sparseVector.getElements().keySet().toArray(new Integer[sparseVector.getElements().size()
                        ])),
                ArrayUtils.toPrimitive(sparseVector.getElements().values().toArray(new Double[sparseVector.getElements()
                        .size()]))
        );

        LOG.info("SparseVector loaded: " + vector);

    }


    @Test
    public void loadFromDF(){
        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "wiig.dia.fi.upm.es")
                .set("spark.cassandra.connection.port", "5011")
                .set("spark.cassandra.input.split.size_in_mb","1280")
//                .set("spark.cassandra.input.fetch.size_in_rows","1000")
//                .set("spark.cassandra.input.split.size","10000")
//                .set("spark.cassandra.output.throughput_mb_per_sec","10")
                ;

        SparkContext sc = new SparkContext("local", "cassandra-test", conf);

        CassandraSQLContext cc = new CassandraSQLContext(sc);

        DataFrame res = cc.sql("select uri,tokens from research.items");

        res.show();

        ObjectMapper jsonMapper = new ObjectMapper();

        List<String> uris = Arrays.asList(new String[]{
                "http://librairy.org/items/fe9b97e0d260bf4d9bd6e77fcd9d7974",
                "http://librairy.org/items/1944a4b80ea68f14633d27f127eb09dd",
                "http://librairy.org/items/20951e06ffd17e91efefa0922a109be8",
                "http://librairy.org/items/ce47b2cbc63c4bdf69fde6c80f31cddd",
                "http://librairy.org/items/c136cadc34511fc9e2c815c8ff4744ea"
        });

        List<Tuple2<Long, Vector>> corpus = res.select("uri").where("uri = 'http://librairy" +
                ".org/items/fe9b97e0d260bf4d9bd6e77fcd9d7974'")
                .toJavaRDD()
                .map(row -> row.getString(row.fieldIndex("uri")))
                .map(uri -> {
                    SparseVector sv = jsonMapper.readValue(new File("/opt/spark/vectors/test.json"), SparseVector
                            .class);
                    Vector vector = Vectors.sparse(
                            sv.getSize(),
                            ArrayUtils.toPrimitive(sv.getElements().keySet().toArray(new Integer[sv.getElements().size()])),
                            ArrayUtils.toPrimitive(sv.getElements().values().toArray(new Double[sv.getElements().size()]))
                    );
                    return vector;
                })
                .zipWithIndex()
                .map(tuple -> tuple.swap())
                .collect()
        ;

        LOG.info("Corpus: " + corpus);

    }


}
