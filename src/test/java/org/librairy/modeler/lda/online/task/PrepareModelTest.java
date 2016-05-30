package org.librairy.modeler.lda.online.task;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.librairy.Application;

/**
 * Created on 17/05/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
public class PrepareModelTest {


    @Test
    public void simulate(){

        String[] args = new String[]{"prepare","10"};

        Application.main(args);


    }

}
