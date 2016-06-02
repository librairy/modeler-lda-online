package org.librairy;

import es.cbadenes.lab.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Created on 29/04/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
public class ApplicationTest {


    @Test
    public void launch(){

        String[] args = new String[]{"train","10","100","1","1"};

        Application.main(args);
    }
}
