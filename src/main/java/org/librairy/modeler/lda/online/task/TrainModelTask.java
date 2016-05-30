package org.librairy.modeler.lda.online.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.mllib.clustering.LDAModel;
import org.librairy.modeler.lda.online.builder.CorpusBuilder;
import org.librairy.modeler.lda.online.builder.FileBuilder;
import org.librairy.modeler.lda.online.builder.ModelBuilder;
import org.librairy.modeler.lda.online.builder.SparkBuilder;
import org.librairy.modeler.lda.online.data.Corpus;
import org.librairy.modeler.lda.online.data.Vocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.io.IOException;

/**
 * Created on 09/05/16:
 *
 * @author cbadenes
 */
public class TrainModelTask {

    private static final Logger LOG = LoggerFactory.getLogger(TrainModelTask.class);

    public static final String BASE_DIRECTORY = "/opt/spark/model";

    private final CorpusBuilder corpusBuilder;
    private final ModelBuilder modelBuilder;
    private final SparkBuilder sparkBuilder;

    public TrainModelTask(ApplicationContext ctx){
        this.corpusBuilder  = ctx.getBean(CorpusBuilder.class);
        this.modelBuilder   = ctx.getBean(ModelBuilder.class);
        this.sparkBuilder   = ctx.getBean(SparkBuilder.class);


    }


    public void run(String[] args) throws IOException {

        Integer size        = 50;
        Integer topics      = 50;
        Integer iterations  = 50;
        Double alpha        = 0.1;
        Double beta         = 0.1;
        Boolean perplexity  = false;



        if (args.length >1) {
            size = Integer.valueOf(args[1]);


            if (args.length > 2){
                topics = Integer.valueOf(args[2]);

                if (args.length > 3){
                    iterations = Integer.valueOf(args[3]);


                    if (args.length > 4){
                        perplexity = Boolean.valueOf(args[4]);


                        if (args.length > 5){
                            alpha = Double.valueOf(args[5]);


                            if (args.length > 6){
                                beta = Double.valueOf(args[6]);
                            }
                        }
                    }
                }
            }
        }


        Corpus corpus = corpusBuilder.newTrainingCorpus(size);

        LDAModel model = modelBuilder.train(corpus,alpha,beta,topics,iterations,perplexity);

        File file = FileBuilder.newFile(BASE_DIRECTORY +"/model-"+size+"-"+topics+"-"+iterations,true);
        LOG.info("Saving the model: " + file.getAbsolutePath());
        model.save(sparkBuilder.sc.sc(),"file://"+file.getAbsolutePath());

    }
}
