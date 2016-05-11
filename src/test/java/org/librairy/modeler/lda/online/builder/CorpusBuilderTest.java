package org.librairy.modeler.lda.online.builder;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.librairy.Application;
import org.librairy.modeler.lda.online.data.Corpus;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

import java.io.IOException;

/**
 * Created on 11/05/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
public class CorpusBuilderTest {


    @Test
    public void large(){


        try {
            ApplicationContext ctx = SpringApplication.run(Application.class, new String[]{});

            CorpusBuilder corpusBuilder = ctx.getBean(CorpusBuilder.class);
            Corpus corpus = corpusBuilder.newTrainingCorpus(500000);

            System.out.println("Vocabulary: " + corpus.getVocabulary().getWords().size());

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
