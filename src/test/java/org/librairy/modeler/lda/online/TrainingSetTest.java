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
public class TrainingSetTest {

    private static final Logger LOG = LoggerFactory.getLogger(TrainingSetTest.class);

    @Autowired
    CitesBuilder citesBuilder;


    public void create() throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        UriSet trainingSet = new UriSet();

        LOG.info("Composing training-set..");
        trainingSet.setUris(citesBuilder.getPatentsWithoutCites().parallelStream().filter(uri -> citesBuilder.getCitesFrom
                (uri).isEmpty())
                .limit
                        (10000)
                .collect(Collectors.toList()));
        LOG.info("Writing training-set  to json file...");
        File trainingFile = new File("src/main/resources/training-set.json");
        jsonMapper.writeValue(trainingFile,trainingSet);
    }

}
