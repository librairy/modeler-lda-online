package org.librairy.modeler.lda.online;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.librairy.modeler.lda.online.data.Vocabulary;

import java.io.File;
import java.io.IOException;

/**
 * Created on 09/05/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
public class VocabularyTest {

    @Test
    public void composeFromJson() throws IOException {


        ObjectMapper jsonMapper = new ObjectMapper();
        File vocabFile = new File("/opt/spark/inbox/vocab-1000-100.json");

        Vocabulary vocabulary = jsonMapper.readValue(vocabFile, Vocabulary.class);

        System.out.println(vocabulary.getWords().size());
        System.out.println(vocabulary.getWordId("coach"));




    }

}
