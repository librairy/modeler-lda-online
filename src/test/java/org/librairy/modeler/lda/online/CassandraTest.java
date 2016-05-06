package org.librairy.modeler.lda.online;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.librairy.Application;
import org.librairy.modeler.lda.online.builder.CorpusBuilder;
import org.librairy.modeler.lda.online.data.Corpus;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

import java.io.IOException;

/**
 * Created on 06/05/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
public class CassandraTest {

    @Test
    public void readRow() throws IOException {

        ApplicationContext ctx = SpringApplication.run(Application.class, new String[]{});

        CorpusBuilder cb = ctx.getBean(CorpusBuilder.class);

        Corpus corpus = cb.newTrainingCorpus(10000);

        System.out.println("Vocabulary Size: " + corpus.getVocabulary().size());

    }
}
