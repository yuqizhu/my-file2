package com.microsoft.azure.storage;

import com.microsoft.azure.storage.blob.*;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Flowable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.util.*;


/**
 * The personnel ingestion application
 */
public class MessageToBlob {

    @Test
    public void basicTest() throws MalformedURLException, InvalidKeyException {
        String kafkaBrokers = System.getenv("KAFKA_BROKERS");
        String kafkaGroupId = System.getenv("KAFKA_GROUP_ID");
        String kafkaTopic = System.getenv("KAFKA_TOPIC");

        Map<String, Object> kafkaParams = new HashMap<>();

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("MessageToBlob");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        kafkaParams.put("bootstrap.servers", kafkaBrokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", kafkaGroupId);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList(kafkaTopic);


        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams, )
                );

//        stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        System.out.print("Connected");

        stream.foreachRDD(rdd ->
        {
            System.out.print("InRDD");

            rdd.foreachPartition(partitionOfRecords -> {
                String accountName = System.getenv("STORAGE_NAME");
                String accountKey = System.getenv("STORAGE_KEY");
                SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);


                HttpPipeline pipeline = StorageURL.createPipeline(credential, new PipelineOptions());

                URL u = new URL(String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName));

                ServiceURL serviceURL = new ServiceURL(u, pipeline);

                ContainerURL containerURL = serviceURL.createContainerURL("trusted2");

                while (partitionOfRecords.hasNext()) {
                    ConsumerRecord<String, String> record = partitionOfRecords.next();
                    String blobName = String.valueOf(record.timestamp());
                    BlockBlobURL blobURL = containerURL.createBlockBlobURL(blobName);
                    byte[] data_bytes = record.value().getBytes();
                    blobURL.upload(Flowable.just(ByteBuffer.wrap(data_bytes)), data_bytes.length,
                            null, null, null, null).blockingGet();
                    System.out.print("Saved");
                }

            });
        }
        );
    }
}



