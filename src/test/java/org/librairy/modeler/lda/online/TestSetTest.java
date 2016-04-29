package org.librairy.modeler.lda.online;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.librairy.modeler.lda.online.builder.CitesBuilder;
import org.librairy.modeler.lda.online.data.UriSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Created on 29/04/16:
 *
 * @author cbadenes
 */
public class TestSetTest {

    private static final Logger LOG = LoggerFactory.getLogger(TestSetTest.class);

    @Autowired
    CitesBuilder citesBuilder;


    public void create() throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        UriSet trainingSet = new UriSet();

        LOG.info("Composing test-set..");
        trainingSet.setUris(citesBuilder.getPatentsWithCites().parallelStream().filter(uri -> citesBuilder.getCitesFrom
                (uri).isEmpty())
                .limit
                        (10000)
                .collect(Collectors.toList()));
        LOG.info("Writing test-set  to json file...");
        File trainingFile = new File("src/main/resources/test-set.json");
        jsonMapper.writeValue(trainingFile,trainingSet);
    }

}
