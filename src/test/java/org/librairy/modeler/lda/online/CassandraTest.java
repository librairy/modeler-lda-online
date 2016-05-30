package org.librairy.modeler.lda.online;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.cbadenes.lab.test.IntegrationTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.librairy.modeler.lda.online.data.BOW;
import org.librairy.modeler.lda.online.data.TextUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created on 06/05/16:
 *
 * @author cbadenes
 */
@Category(IntegrationTest.class)
public class CassandraTest {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraTest.class);

    @Test
    public void readRow() throws IOException, InterruptedException {

        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "wiig.dia.fi.upm.es")
                .set("spark.cassandra.connection.port", "5011")
                .set("spark.cassandra.input.split.size_in_mb","1280")
//                .set("spark.cassandra.input.fetch.size_in_rows","1000")
//                .set("spark.cassandra.input.split.size","10000")
//                .set("spark.cassandra.output.throughput_mb_per_sec","10")
                ;

        SparkContext sc = new SparkContext("local", "cassandra-test", conf);

        CassandraSQLContext cc = new CassandraSQLContext(sc);

        DataFrame res = cc.sql("select uri,tokens from research.items");

        res.show();

        List<String> uris = Arrays.asList(new String[]{
                "http://librairy.org/items/fe9b97e0d260bf4d9bd6e77fcd9d7974",
                "http://librairy.org/items/1944a4b80ea68f14633d27f127eb09dd",
                "http://librairy.org/items/20951e06ffd17e91efefa0922a109be8",
                "http://librairy.org/items/ce47b2cbc63c4bdf69fde6c80f31cddd",
                "http://librairy.org/items/c136cadc34511fc9e2c815c8ff4744ea"
        });

        List<Row> rows = res.select("uri").limit(100).collectAsList();


        ObjectMapper jsonMapper = new ObjectMapper();

//        uris.parallelStream().forEach(uri -> {

        rows.parallelStream().forEach(row -> {

            String uri = row.getString(row.fieldIndex("uri"));
            LOG.info("Building bow of " + uri);
            BOW bow = new BOW();
            Map<String, Long> freq = res.select("uri","tokens").where("uri = '"+uri+"'")
                    .toJavaRDD()
                    .flatMap(r -> Arrays.asList(r.getString(r.fieldIndex("tokens")).split(" ")))
                    .mapToPair(word -> new Tuple2<String, Long>(word, 1l))
                    .reduceByKey((x, y) -> x + y)
                    .collectAsMap();
            bow.setFrequencies(freq);

            String name = StringUtils.substringAfterLast(uri,"/");

            LOG.info("Serializing bow of " + uri);
            try {
                jsonMapper.writeValue(new File("/opt/spark/bows/"+name+".json"),bow);
            } catch (IOException e) {
                LOG.error("Error writing json", e);
            }
        });


        LOG.info("Completed!");

    }
}
