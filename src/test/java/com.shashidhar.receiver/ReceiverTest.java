package com.shashidhar.receiver;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaTestUtils;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by shashidhar on 19/1/16.
 */
public class ReceiverTest implements Serializable
{

    private transient JavaStreamingContext ssc = null;
    private transient Random random = new Random();
    private transient KafkaTestUtils kafkaTestUtils = null;

    @Before
    public void setUp() {
        kafkaTestUtils = new KafkaTestUtils();
        kafkaTestUtils.setup();
        SparkConf sparkConf = new SparkConf()
                        .setMaster("local[4]").setAppName(this.getClass().getSimpleName());
        ssc = new JavaStreamingContext(sparkConf, new Duration(500));
    }


    @After
    public void tearDown() {
        if (ssc != null) {
            ssc.stop();
            ssc = null;
        }

        if (kafkaTestUtils != null) {
            kafkaTestUtils.teardown();
            kafkaTestUtils = null;
        }
    }

    @Test
    public void testKafkaStream() throws InterruptedException {
        String topic = "topic1";
        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put(topic, 1);

        String[] sent = new String[3];
        sent[0] = "first";
        sent[1] = "second";
        sent[2] = "third";

        final List<String> sentList = new ArrayList<String>();
        sentList.addAll(Arrays.asList(sent));

        kafkaTestUtils.createTopic(topic);
        kafkaTestUtils.sendMessages(topic, sent);

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("zookeeper.connect", kafkaTestUtils.zkAddress());
        kafkaParams.put("group.id", "test-consumer-" + random.nextInt(10000));
        kafkaParams.put("auto.offset.reset", "smallest");

        final List<String> received = new ArrayList<String>();

        JavaDStream<String> stream = KafkaUtils.createStream(ssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topics,
                        StorageLevel.MEMORY_ONLY_SER())
                        .map(new Function<Tuple2<String, String>, String>()
                        {
                            public String call(Tuple2<String, String> stringStringTuple2) throws Exception
                            {
                                return stringStringTuple2._2();
                            }
                        });

        stream.foreachRDD(new Function<JavaRDD<String>, Void>()
        {
            public Void call(final JavaRDD<String> v1) throws Exception
            {
                received.addAll(v1.collect());
                return null;
            }
        });

        ssc.start();

        long startTime = System.currentTimeMillis();
        boolean sizeMatches = false;
        while (!sizeMatches && System.currentTimeMillis() - startTime < 20000) {
            sizeMatches = sentList.size() == received.size();
            Thread.sleep(200);
        }
        Assert.assertEquals(sentList.size(), received.size());
    }

}
