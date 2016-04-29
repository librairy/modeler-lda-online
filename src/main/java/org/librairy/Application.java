package org.librairy;

import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.librairy.modeler.lda.online.builder.CorpusBuilder;
import org.librairy.modeler.lda.online.builder.ModelBuilder;
import org.librairy.modeler.lda.online.builder.SparkBuilder;
import org.librairy.modeler.lda.online.data.Corpus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created on 28/04/16:
 *
 * @author cbadenes
 */
@SpringBootApplication
//@EnableAutoConfiguration
//@EnableConfigurationProperties
public class Application {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args){


        try {
            ApplicationContext ctx = SpringApplication.run(Application.class, args);

            Integer size        = 50;
            Double alpha        = 0.1;
            Double beta         = 0.1;
            Integer topics      = 50;
            Integer iterations  = 50;

            if (args != null){

                if (args.length >0) {
                    size = Integer.valueOf(args[0]);


                    if (args.length > 1){
                        topics = Integer.valueOf(args[1]);

                        if (args.length > 2){
                            iterations = Integer.valueOf(args[2]);


                            if (args.length > 3){
                                alpha = Double.valueOf(args[3]);


                                if (args.length > 4){
                                    beta = Double.valueOf(args[4]);
                                }
                            }

                        }

                    }

                }


            }


            CorpusBuilder corpusBuilder = ctx.getBean(CorpusBuilder.class);
            Corpus corpus = corpusBuilder.newTrainingCorpus(size);

            ModelBuilder modelBuilder = ctx.getBean(ModelBuilder.class);
            LocalLDAModel model = modelBuilder.newModel(corpus,alpha,beta,topics,iterations);

            File file = new File("model-"+topics);
            if (file.exists()) Files.delete(Paths.get(file.getAbsolutePath()));

            LOG.info("Saving the model: " + file.getAbsolutePath());
            SparkBuilder sparkBuilder = ctx.getBean(SparkBuilder.class);
            model.save(sparkBuilder.sc.sc(),file.getAbsolutePath());

            System.exit(0);
        } catch (Exception e) {
            LOG.error("Error executing test",e);
            System.exit(-1);
        }

    }
}
