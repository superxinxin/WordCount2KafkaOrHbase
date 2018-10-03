package com.demo.SparkStreamingWordCount2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flume.source.avro.AvroFlumeEvent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class SparkStreamingWordCount1
{
	public static void main(String[] args) throws InterruptedException
	{
		String topics = "GSMtest1";
		Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, Object> kafkaParams = kafkaConf(topics);
		HashMap offsets = kafkaOffset(topics);
		JavaStreamingContext ssc = sparkStreamingConf();
		JavaInputDStream<ConsumerRecord<Object, Object>> lines = kafkaMethod(ssc, topicsSet, kafkaParams, offsets);
		lines.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<Object,Object>>>()
		{
			@Override
			public void call(JavaRDD<ConsumerRecord<Object, Object>> t) throws Exception
			{
				t.foreach(new VoidFunction<ConsumerRecord<Object,Object>>()
						{
							@Override
							public void call(ConsumerRecord<Object, Object> t) throws Exception
							{
								String stream = (String)t.value();
								if(stream != null)
								{
									getHeaders(stream);
								}
							}
						});
			}
		});
		lines.print();
//		JavaPairDStream<String, Integer> counts1 = JavaPairDStreamMethod(lines);
//		counts1.print();
//		JavaDStream2Kafka(counts1);
		ssc.start();
		ssc.awaitTermination();
		ssc.close();
	}
	
	public static void getHeaders(String stream)
	{
	    AvroFlumeEvent result = null;
	    ByteBuffer data = null;
	    Map<CharSequence, CharSequence> map = null;
	 
	    byte[] bytes = stream.getBytes();
	    /**解码还原为flumeEvent**/
	    SpecificDatumReader<AvroFlumeEvent> reader = new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class);
	 
	    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
	    try{
	        result = reader.read(null, decoder);
	        map = result.getHeaders();
	        data = result.getBody();
	    }catch (IOException e){
	        e.printStackTrace();
	    }
	    catch (Exception e)
	    {
	        e.printStackTrace();
	        return;
	    }
	    /**输出header信息和body信息**/
	    System.out.println("header: ");
	    for (Map.Entry<CharSequence, CharSequence>entry: map.entrySet()){
	        System.out.println(entry.getKey() + " : " + entry.getValue());
	    }
	    String bodyData = new String(data.array());
	    System.out.println("body: " + bodyData);
	}
	
	
	public static Map<String, Object> kafkaConf(String topics)
	{
		String brokers = "10.21.20.42:9092";
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("group.id", "test-consumer-group2");
		kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("auto.offset.reset", "earliest");
		return kafkaParams;
	}
	public static HashMap kafkaOffset(String topics)
	{
		HashMap offsets = new HashMap<>();
//		offsets.put(new TopicPartition(topics, 0), 2L);
		return offsets;
	}
	public static JavaStreamingContext sparkStreamingConf()
	{
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("streaming word count");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));
		return ssc;
	}
	public static JavaInputDStream<ConsumerRecord<Object, Object>> kafkaMethod
	(JavaStreamingContext ssc, Collection<String> topicsSet, Map<String, Object> kafkaParams, HashMap offsets)
	{
		JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(ssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams, offsets));
		return lines;
	}
	public static JavaPairDStream<String, Integer> sparkStreamingMethod(JavaInputDStream<ConsumerRecord<Object, Object>> lines)
	{
		JavaPairDStream<String, Integer> counts =
		lines.flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
		.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);
		return counts;
	}
	public static JavaPairDStream<String, Integer> JavaPairDStreamMethod(JavaInputDStream<ConsumerRecord<Object, Object>> lines)
	{
		JavaDStream<String> counts2 = 
				lines.flatMap(x -> Arrays.asList(x.value().toString()).iterator());
		JavaPairDStream<String, Integer> counts3 = 
				counts2.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);
		return counts3;
	}
	public static void JavaDStream2Kafka(JavaPairDStream<String, Integer> counts1)
	{
		counts1.foreachRDD(
                (VoidFunction<JavaPairRDD<String, Integer>>) values -> {
                    values.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                        @Override
                        public void call(Tuple2<String, Integer> tuple) throws Exception {
                           // data2Kafka(tuple);
                        	System.out.println("counts:" + tuple._1() 
                            +" - "+ tuple._2());
                        	HbaseCon hbase = new HbaseCon();
                        	hbase.inserttest(tuple._1());
                        }
                    });
                });
	}
//	public static void data2Kafka(Tuple2<String, Integer> tuple)
//	{
//		String topic = "mytopic7";
//		String messageStr = "counts:"+tuple._1()+" - "+tuple._2();
//		Properties props = new Properties();
//		props.put("metadata.broker.list", "zk2:9092");
//		props.put("bootstrap.servers", "zk2:9092");
//		props.put("acks", "all");
//		props.put("retries", 0);
//		props.put("batch.size", 16384);
//		props.put("linger.ms", 1);
//		props.put("buffer.memory", 33554432);
//		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
//		producer.send(new ProducerRecord<Integer, String>(topic,messageStr));
//	}	
}
//class Event
//{
//	private Map<String, String> headers;
//	private byte[] body;
//	public Map<String, String> getHeaders()
//	{
//		return headers;
//	}
//	public byte[] getBody()
//	{
//		return body;
//	}
//}




