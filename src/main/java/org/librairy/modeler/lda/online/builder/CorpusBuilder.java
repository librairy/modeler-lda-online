package org.librairy.modeler.lda.online.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.storage.StorageLevel;
import org.librairy.modeler.lda.online.data.BOW;
import org.librairy.modeler.lda.online.data.Corpus;
import org.librairy.modeler.lda.online.data.SparseVector;
import org.librairy.modeler.lda.online.data.Vocabulary;
import org.librairy.modeler.lda.online.task.PrepareModelTask;
import org.librairy.modeler.lda.online.task.TrainModelTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 28/04/16:
 *
 * @author cbadenes
 */
@Component
public class CorpusBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(CorpusBuilder.class);

    @Autowired
    private CitesBuilder citesBuilder;

    @Value("${training.file}")
    private org.springframework.core.io.Resource trainingFile;

    @Value("${test.file}")
    private org.springframework.core.io.Resource testFile;

    @Autowired
    private SparkBuilder sparkBuilder;


    public Corpus newTrainingCorpus(Integer size) throws IOException {
        return newCorpus(size,"vocab-"+size);
    }

    public Corpus newCorpus(Integer size, String refVocabulary ) throws IOException {

        LOG.info("Corpus of " + size + " documents");

        CassandraSQLContext cc = new CassandraSQLContext(sparkBuilder.sc.sc());

        String sql = "select uri,tokens from research.items";

        LOG.info("Getting texts from Cassandra by '" + sql + "'");

        DataFrame res = cc.sql(sql);

        res.show();

        LOG.info("DataFrame loaded");
        ObjectMapper jsonMapper = new ObjectMapper();

        // Load Vocabulary
        File vocabFile = new File(PrepareModelTask.VOCAB_FOLDER+"vocab-"+size+".json");
        if (!vocabFile.exists()) throw new RuntimeException("Vocabulary file does not exist! " + vocabFile
                .getAbsolutePath());

        LOG.info("Reading vocabulary from json file: " + vocabFile.getAbsolutePath());
        Vocabulary vocabulary = jsonMapper.readValue(vocabFile, Vocabulary.class);

        LOG.info("Broadcasting vocabulary..");
        Broadcast<Vocabulary> vocabularyB = sparkBuilder.sc.broadcast(vocabulary);

        LOG.info("Creating (sparse) vectors from BOWs..");
        JavaPairRDD<Long, Vector> vectors = res.select("uri").limit(size)
                .toJavaRDD()
                .map(row -> row.getString(row.fieldIndex("uri")))
                .map(uri -> {
                    String name = StringUtils.substringAfterLast(uri, "/");
                    BOW bow = jsonMapper.readValue(new File(PrepareModelTask
                            .BOW_FOLDER + name + ".json"), BOW.class);

                    return VectorBuilder.from(bow,vocabularyB.getValue());
//
//
//                    Vector vector = Vectors.sparse(
//                            sv.getSize(),
//                            ArrayUtils.toPrimitive(sv.getElements().keySet().toArray(new Integer[sv.getElements().size()])),
//                            ArrayUtils.toPrimitive(sv.getElements().values().toArray(new Double[sv.getElements().size()]))
//                    );
//                    return vector;
                })
                .zipWithIndex()
                .mapToPair(tuple -> tuple.swap())
                .cache();

        Corpus corpus = new Corpus();
        corpus.setBagsOfWords(vectors);
//        corpus.setVocabulary(vocabulary);
        corpus.setSize(size);
        corpus.setVocabularySize(vocabulary.getSize());
        return corpus;

    }

}
