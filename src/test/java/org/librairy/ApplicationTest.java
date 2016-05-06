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

        String[] args = new String[]{"2000","100","100"};

        Application.main(args);
    }
}
