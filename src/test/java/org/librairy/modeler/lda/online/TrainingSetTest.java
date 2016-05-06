package org.librairy.modeler.lda.online;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.cbadenes.lab.test.IntegrationTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.librairy.Application;
import org.librairy.model.domain.resources.Resource;
import org.librairy.modeler.lda.online.builder.CitesBuilder;
import org.librairy.modeler.lda.online.data.UriSet;
import org.librairy.storage.UDM;
import org.librairy.storage.system.column.repository.UnifiedColumnRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

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
        "librairy.neo4j.port = 5032",
        "librairy.eventbus.host = zavijava.dia.fi.upm.es",
        "librairy.eventbus.port = 5041"
})
public class TrainingSetTest {

    private static final Logger LOG = LoggerFactory.getLogger(TrainingSetTest.class);

    @Autowired
    CitesBuilder citesBuilder;

    @Autowired
    UDM udm;

    @Autowired
    UnifiedColumnRepository columnRepository;


    public void create() throws IOException {
//        ObjectMapper jsonMapper = new ObjectMapper();
//        UriSet trainingSet = new UriSet();
//
//
//        columnRepository.findBy(Resource.Type.DOCUMENT, )
//
//        LOG.info("Composing training-set..");
//        trainingSet.setUris(citesBuilder.getPatentsWithoutCites().parallelStream().filter(uri -> citesBuilder.getCitesFrom
//                (uri).isEmpty())
//                .limit
//                        (10000)
//                .collect(Collectors.toList()));
//        LOG.info("Writing training-set  to json file...");
//        File trainingFile = new File("src/main/resources/training-set.json");
//        jsonMapper.writeValue(trainingFile,trainingSet);
    }

}
