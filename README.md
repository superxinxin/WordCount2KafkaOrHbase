# WordCount2KafkaOrHbase
## 实现功能介绍：
SparkStreaming从kafka获取数据，数据格式是flume的Event对象。SparkStreaming流计算获得对象，然后实现WordCount，再将WordCount的结果RDD写入Hbase或Kafka的topic中。
## HbaseCon.java
这个HbaseDemo写的很简单，主要就是用SQL语句插入Phoenix，也就是插入Hbase。
## SparkStreamingWordCount1.java
### 简单介绍：
在原有WordCount代码基础上做了一些改进。可以参考![SparkAndKafka](https://github.com/superxinxin/SparkAndKafka)。因为SparkStreaming是从Kafka中获得数据，数据格式可以是简单的String类型。但是，在我的实习项目中，kafka数据是flume从Hbase进行日志采集后放入kafka的，kafka中的数据格式是Event对象类型。所以这里写了个Demo。
### 主要工作：
* 1）从kafka获得JavaRDD流数据lines
* 2）对lines使用foreach算子，调用getHeaders方法，将lines解码还原为flumeEvent,然后输出header信息和body信息。这里header放了分隔符，body放了一条记录。如header中放了“，”，body中是一条记录“a1，b2，c3，d4，e5”。
```Java
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
```
* 3）后续操作就是WordCount，得到JavaPairRDD类型对象counts。
* 4）然后执行JavaDStream2Kafka方法，在这个方法中可以调用两个方法
  * data2Kafka方法：将统计数据放入kafka
  * hbase.inserttest方法：将统计数据插入Hbase。
