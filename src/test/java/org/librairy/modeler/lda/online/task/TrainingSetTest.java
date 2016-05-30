package org.librairy.modeler.lda.online.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.Application;
//import org.librairy.model.domain.relations.Relation;
//import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.online.builder.CitesBuilder;
import org.librairy.modeler.lda.online.data.UriSet;
//import org.librairy.storage.UDM;
//import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created on 29/04/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@TestPropertySource(properties = {
        "librairy.cassandra.contactpoints = zavijava.dia.fi.upm.es",
        "librairy.cassandra.port = 5011",
        "librairy.cassandra.keyspace = research",
        "librairy.elasticsearch.contactpoints = zavijava.dia.fi.upm.es",
        "librairy.elasticsearch.port = 5021",
        "librairy.neo4j.contactpoints = zavijava.dia.fi.upm.es",
        "librairy.neo4j.port = 5030",
        "librairy.eventbus.host = zavijava.dia.fi.upm.es",
        "librairy.eventbus.port = 5041"
})
public class TrainingSetTest {

    private static final Logger LOG = LoggerFactory.getLogger(TrainingSetTest.class);

    @Autowired
    CitesBuilder citesBuilder;

//    @Autowired
//    UnifiedColumnRepository columnRepository;
//
//    @Autowired
//    UDM udm;


    @Test
    public void create() throws IOException {



//        Iterable<Relation> iterable = columnRepository.findBy(Relation.Type.CONTAINS, "domain", "http://librairy.org/domains/f69d397b7bd675645769ec0a561b8d8b");
//        List<String> documentUris = StreamSupport.
//                stream(iterable.spliterator(), false).
//                parallel().
//                map(relation -> relation.getEndUri()).
//                collect(Collectors.toList());

//        List<String> documentUris = udm.find(Resource.Type.DOCUMENT).from(Resource.Type.DOMAIN, "http://librairy" +
//                ".org/domains/default");


//        trainingSet.setUris(citesBuilder.getPatentsWithoutCites().parallelStream().filter(uri -> citesBuilder.getCitesFrom
//                (uri).isEmpty())
//                .limit
//                        (10000)
//                .collect(Collectors.toList()));

//        createFile(documentUris,800000);
//        createFile(documentUris,500000);
//        createFile(documentUris,100000);
//        createFile(documentUris,50000);
//        createFile(documentUris,10000);
//        createFile(documentUris,1000);

    }

    private void createFile(List<String> documentUris, Integer size) throws IOException {
        LOG.info("Composing training-set for " + size);
        ObjectMapper jsonMapper = new ObjectMapper();
        UriSet trainingSet = new UriSet();
        Integer realSize = size;
        if (size > documentUris.size()){
            realSize = documentUris.size();
            trainingSet.setUris(documentUris);
        }else{
            trainingSet.setUris(documentUris.subList(0,size));
        }
        LOG.info("Writing training-set  to json file...");
        File trainingFile = new File("src/main/resources/training-set-"+(realSize/1000)+"k.json");
        jsonMapper.writeValue(trainingFile,trainingSet);
    }

}
