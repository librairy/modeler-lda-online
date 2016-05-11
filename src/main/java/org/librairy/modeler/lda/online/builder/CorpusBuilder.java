package org.librairy.modeler.lda.online.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.storage.StorageLevel;
import org.librairy.model.domain.relations.Relation;
import org.librairy.model.domain.resources.Item;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.online.data.Corpus;
import org.librairy.modeler.lda.online.data.UriSet;
import org.librairy.modeler.lda.online.data.Vocabulary;
import org.librairy.storage.UDM;
import org.librairy.storage.executor.ParallelExecutor;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created on 28/04/16:
 *
 * @author cbadenes
 */
@Component
public class CorpusBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(CorpusBuilder.class);

    private LoadingCache<String, String> cache;

    @Autowired
    private UDM udm;

    @Autowired
    private UnifiedColumnRepository unifiedColumnRepository;

    @Autowired
    private CitesBuilder citesBuilder;

    @Value("${training.file}")
    private org.springframework.core.io.Resource trainingFile;

    @Value("${test.file}")
    private org.springframework.core.io.Resource testFile;

    @Autowired
    private SparkBuilder sparkBuilder;

    @PostConstruct
    public void setup(){
        LOG.info("Creating initial doc-cache...");
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, String>() {
                            public String load(String uri) {
                                Iterator<Relation> it = unifiedColumnRepository.findBy(Relation.Type.BUNDLES,
                                        "document", uri).iterator();
                                if (!it.hasNext()){
                                    List<String> res = udm.find(Resource.Type.ITEM).from(Resource.Type.DOCUMENT, uri);
                                    if (!res.isEmpty()){
                                        udm.save(Relation.newBundles(uri, res.get(0)));
                                        return res.get(0);
                                    }
                                    return "";
                                }
                                return it.next().getEndUri();
                            }
                        });
    }


    public Corpus newTrainingCorpus(Integer size) throws IOException {

        ObjectMapper jsonMapper = new ObjectMapper();
        UriSet trainingSet = new UriSet();
        String fileName = "/training-set-"+(size/1000)+"k.json";
        InputStream is = this.getClass().getResourceAsStream(fileName);
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        trainingSet = jsonMapper.readValue(in,UriSet.class);
        List<String> uris = trainingSet.getUris();
        return newCorpus(uris,null);
    }


    public Corpus newTestCorpus(Integer size) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        UriSet trainingSet = new UriSet();
        BufferedReader in = new BufferedReader(new InputStreamReader(testFile.getInputStream()));
        trainingSet = jsonMapper.readValue(in,UriSet.class);
        List<String> uris = trainingSet.getUris().subList(0, size);
        return newCorpus(uris,null);
    }

    private Corpus newCorpus(List<String> uris, Map<String, Long> refVocabulary ){

        LOG.info("Composing corpus from: " + uris.size() + " documents ...");

        CopyOnWriteArrayList<Tuple2<String, Map<String, Long>>> resources = new CopyOnWriteArrayList<>();

        ParallelExecutor executor = new ParallelExecutor();
        for(String uri : uris){

            executor.execute(() -> {
                String itemUri = null;
                try {
//                    LOG.info("Getting item from: " + uri);
                    itemUri = cache.get(uri);
                    Optional<Resource> res = udm.read(Resource.Type.ITEM).byUri(itemUri);
                    if (!res.isPresent()) throw new ExecutionException(new RuntimeException("Item not found"));
                    Item item = res.get().asItem();
                    //Default tokenizer
                    Map<String, Long> bow = BowBuilder.count(Arrays.asList(item.getTokens().split(" ")));
                    resources.add(new Tuple2<>(item.getUri(),bow));
                } catch (ExecutionException e) {
                    LOG.warn("Error getting item from document uri: " + uri,e);
                }
            });
        }

        // Tell threads to finish off.
        executor.shutdown();
        // Wait for everything to finish.
        while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            LOG.info("Awaiting completion of threads.");
        }
//        LOG.info("Size of resources: " + SizeEstimator.estimate(resources));



        JavaRDD<Tuple2<String, Map<String, Long>>> itemsRDD = sparkBuilder.sc.parallelize(resources);
        itemsRDD.cache();
//        itemsRDD.persist(StorageLevel.MEMORY_ONLY()); //MEMORY_ONLY_SER

//        LOG.info("Size of ItemsRDD: " + SizeEstimator.estimate(itemsRDD));

        LOG.info("Retrieving the Vocabulary...");

        Broadcast<Map<String, Long>> vocabularyBroadcast = sparkBuilder.sc.broadcast(
                (refVocabulary != null) ? refVocabulary :
                        itemsRDD.
                                flatMap(resource -> resource._2.keySet()).
                                distinct().
                                zipWithIndex().
                                collectAsMap());

//        final Map<String, Long> vocabulary = (refVocabulary != null)? refVocabulary :
//                itemsRDD.
//                        flatMap(resource -> resource._2.keySet()).
//                        distinct().
//                        zipWithIndex().
//                        collectAsMap();

        LOG.info( vocabularyBroadcast.getValue().size() + " words" );

        LOG.info("Indexing the documents...");
        Map<Long, String> documents = itemsRDD.
                map(resource -> resource._1).
                zipWithIndex().
                mapToPair(x -> new Tuple2<Long, String>(x._2, x._1)).
                collectAsMap();

        LOG.info( documents.size() + " documents" );

        LOG.info("Building the Corpus...");

        JavaPairRDD<Long, Vector> bagsOfWords = itemsRDD.
                map(resource -> BowBuilder.from(resource._2,vocabularyBroadcast.getValue())).
                zipWithIndex().
                mapToPair(x -> new Tuple2<Long, Vector>(x._2, x._1));


        Vocabulary vocabulary = new Vocabulary();
        vocabulary.setWords(vocabularyBroadcast.getValue());

        Corpus corpus = new Corpus();
        corpus.setBagsOfWords(bagsOfWords.collectAsMap());
        corpus.setVocabulary(vocabulary);
        corpus.setDocuments(documents);
        return corpus;


    }

}
