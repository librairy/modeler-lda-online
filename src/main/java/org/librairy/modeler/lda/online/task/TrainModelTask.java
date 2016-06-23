package org.librairy.modeler.lda.online.task;

import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.spark.mllib.clustering.LDAModel;
import org.librairy.modeler.lda.online.builder.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created on 09/05/16:
 *
 * @author cbadenes
 */
public class TrainModelTask {

    private static final Logger LOG = LoggerFactory.getLogger(TrainModelTask.class);

    public static final String BASE_DIRECTORY = "/opt/spark/model";

    private final CorpusBuilder corpusBuilder;
    private final SparkBuilder sparkBuilder;
    private final ModelBuilder modelBuilder;

    public TrainModelTask(ApplicationContext ctx, ModelBuilder modelBuilder){
        this.corpusBuilder  = ctx.getBean(CorpusBuilder.class);
        this.sparkBuilder   = ctx.getBean(SparkBuilder.class);
        this.modelBuilder   = modelBuilder;
    }


    public void run(String[] args) throws IOException {

        Integer size        = 50;
        Integer vocabSize   = 100;
        Integer topics      = 50;
        Integer iterations  = 50;
        Double alpha        = 0.1;
        Double beta         = 0.1;
        Boolean perplexity  = false;


        if (args.length >1) {
            size = Integer.valueOf(args[1]);


            if (args.length > 2){
                vocabSize = Integer.valueOf(args[2]);

                if (args.length > 3){
                    topics = Integer.valueOf(args[3]);

                    if (args.length > 4){
                        iterations = Integer.valueOf(args[4]);
                    }
                }

            }

        }


        //Corpus corpus = corpusBuilder.newTrainingCorpus(size);

        try{

            LDAModel model = modelBuilder.train(size,vocabSize,alpha,beta,topics,iterations,perplexity);


            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmm");
            String date = formatter.format(new Date());

            String name = "/model-"+size+"-"+vocabSize+"-"+topics+"-"+iterations+"-"+date;
            LOG.info("Saving the model: " + name);
            model.save(sparkBuilder.sc.sc(),"hdfs://zavijava.dia.fi.upm.es/patents/tic/models/"+name);
        }catch (Exception e){
            if (e instanceof FileAlreadyExistsException) {
                LOG.warn(e.getMessage());
            }else {
                LOG.error("Error building model", e);
                throw e;
            }
        }

    }
}
