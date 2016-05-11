package org.librairy.modeler.lda.online.task;

import org.librairy.modeler.lda.online.builder.CorpusBuilder;
import org.librairy.modeler.lda.online.builder.ModelBuilder;
import org.librairy.modeler.lda.online.builder.SparkBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.io.IOException;

/**
 * Created on 09/05/16:
 *
 * @author cbadenes
 */
public class TestModelTask {

    private static final Logger LOG = LoggerFactory.getLogger(TestModelTask.class);

    private static final String BASE_DIRECTORY = "/opt/spark/inbox";

    private final CorpusBuilder corpusBuilder;
    private final ModelBuilder modelBuilder;
    private final SparkBuilder sparkBuilder;

    public TestModelTask(ApplicationContext ctx){
        this.corpusBuilder  = ctx.getBean(CorpusBuilder.class);
        this.modelBuilder   = ctx.getBean(ModelBuilder.class);
        this.sparkBuilder   = ctx.getBean(SparkBuilder.class);


    }


    public void run(String[] args) throws IOException {



    }
}
