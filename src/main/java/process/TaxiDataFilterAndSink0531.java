/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package process;

import ch.hsr.geohash.GeoHash;
import model.TrajPoint;
import model.avro.SimplePointAvro;
import model.avro.TrajPointAvro;
import model.avro.TrajSegmentAvro;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class TaxiDataFilterAndSink0531 {
	public static int MAX_DELAY_SECONDS=120;
	public static int GEOHASH_PRECISION=8;
	private static Admin admin;
	private static  org.apache.hadoop.conf.Configuration conf = null;
	private static Connection conn = null;

	public static void main(String[] args) throws Exception {


		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(10*1000);
		env.getConfig().setAutoWatermarkInterval(10*1000);
		env.setStateBackend(new FsStateBackend("file:///media/xiaokeai/Study/data/flinkstate"));

		//kafka config
		Properties props=new Properties();
		props.put("bootstrap.servers","localhost:9092");
		props.put("group.id","flinkProcess");
		props.put("auto.offset.reset","earliest");
		props.put("key.deserializer","org.apach.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apach.kafka.common.serialization.StringDeserializer");

		//create consumer
        FlinkKafkaConsumer consumer=new FlinkKafkaConsumer<String>("taxi"
				,new SimpleStringSchema(),props);
        consumer.setStartFromEarliest();
        consumer.assignTimestampsAndWatermarks(new TSExtractor());
        //stream processing
		//read from kafka and transfer to object

		DataStream<String>messageStream=env.addSource(consumer);



		DataStream<TrajPointAvro>trajPointStream=messageStream.map(x->Utils.string2TrajPointAvro(x));
		DataStream<TrajPointAvro>filtered=trajPointStream.filter(new RangeFilter());

		//filtered.addSink(new trajPointHBaseSink());

		//DataStream<Tuple2<String,SimplePointAvro>> simplepoints= filtered.map(new TrajPoint2SimplePoint());
//        initHBase();
//		simplepoints.addSink(new simplePointHBaseSink());
		//simplepoints.print();


		Map<String, String> config = new HashMap<>();
		config.put("bulk.flush.max.actions", "1");   // flush inserts after every event
		config.put("cluster.name", "elasticsearch"); // default cluster name

		List<InetSocketAddress> transports = new ArrayList<>();
// set default connection details
		transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));
		filtered.keyBy(x->x.getTaxiId()).map(new StatusChange()).filter(x->x.f3!=0).addSink(new ElasticsearchSink<>(config,transports,new ChangePointsInserter()));
		//simplepoints.addSink(new ElasticsearchSink<>(config,transports,new PointsInserter()));

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}


	public static class RangeFilter implements FilterFunction<TrajPointAvro>
	{

		@Override
		public boolean filter(TrajPointAvro trajPoint) throws Exception {
			double lon=trajPoint.getLon();
			double lat=trajPoint.getLat();
			double speed=trajPoint.getSpeed();
			if (lon >=113.8499 && lon<=114.8854 && lat>=30.3071 && lat <=30.8692)
				return true;
			else return false;
//
//			if(speed>0) return true;
//			else return false;
		}
	}

	public static class TSExtractor extends BoundedOutOfOrdernessTimestampExtractor<String>
	{
		@Override
		public long extractTimestamp(String line) {

			return Utils.date2TimeStamp(line.split(",")[1],"yyyy-MM-dd HH:mm:ss");
		}
		public TSExtractor() {
			super(Time.seconds(MAX_DELAY_SECONDS));
		}
	}

	public static class TrajPoint2SimplePoint implements MapFunction<TrajPointAvro,Tuple2<String,SimplePointAvro>>
	{

		@Override
		public Tuple2<String,SimplePointAvro> map(TrajPointAvro trajPoint) throws Exception {
			String rowkey=String.valueOf(trajPoint.getCellID())+Utils.timeStamp2Date(trajPoint.getUtc(),"yyyyMMddHHmm");
			String TrajID= Utils.timeStamp2Date(trajPoint.getUtc(),"yyyyMMdd")+"-"+String.valueOf(trajPoint.getTaxiId());
			return new Tuple2<String,SimplePointAvro>(rowkey,new SimplePointAvro(TrajID,trajPoint.getLon(),trajPoint.getLat(),trajPoint.getUtc()));
		}
	}
	private static class myReduce implements ReduceFunction<Tuple2<String,SimplePointAvro>>
	{

		@Override
		public Tuple2<String, SimplePointAvro> reduce(Tuple2<String, SimplePointAvro> t0, Tuple2<String, SimplePointAvro> t1) throws Exception {
			return t0;
		}
	}


	private static class assignRowkey extends ProcessWindowFunction<Tuple2<String,SimplePointAvro>,Tuple2<String,SimplePointAvro>,String,TimeWindow>
	{

		@Override
		public void process(String key, Context context, Iterable<Tuple2<String,SimplePointAvro>> iter, Collector<Tuple2<String, SimplePointAvro>> collector) throws Exception {
			Tuple2<String,SimplePointAvro> pointWithCellID=iter.iterator().next();
			String rowkey=pointWithCellID.f0 +Utils.timeStamp2Date(context.window().getStart(),"yyyyMMddHHmm");
			collector.collect(new Tuple2<>(rowkey,pointWithCellID.f1));
		}
	}


	private static class StatusChange extends RichMapFunction<TrajPointAvro, Tuple4<Long, Double, Double, Integer>>
	{
		private transient ValueState<Integer> status;

		@Override
		public Tuple4<Long, Double, Double, Integer> map(TrajPointAvro point) throws Exception {
			int previous=status.value();
			status.update(point.getPassenger());
			return new Tuple4<>(point.getUtc(),point.getLat(),point.getLon(),point.getPassenger()-previous);

		}

		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Integer>descriptor=new ValueStateDescriptor<>("passengerStatus"
					, Integer.TYPE
					,0);
			status=getRuntimeContext().getState(descriptor);
		}
	}

	private static class PointsInserter implements ElasticsearchSinkFunction<Tuple2<String,SimplePointAvro>>
	{

		@Override
		public void process(Tuple2<String, SimplePointAvro> record, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
			Map<String,String> json=new HashMap<>();
			json.put("time",String.valueOf(record.f1.getUtc()));
			json.put("location",String.valueOf(record.f1.getLat())+","+String.valueOf(record.f1.getLon()));
			IndexRequest rqst= Requests.indexRequest()
					.index("taxi")
					.type("points")
					.source(json);
			requestIndexer.add(rqst);
		}
	}

	private static class ChangePointsInserter implements ElasticsearchSinkFunction<Tuple4<Long,Double,Double,Integer>>
	{

		@Override
		public void process(Tuple4<Long,Double,Double,Integer>record, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
			Map<String,String> json=new HashMap<>();
			json.put("time",String.valueOf(record.f0));
			json.put("location",String.valueOf(record.f1)+","+String.valueOf(record.f2));
			IndexRequest rqst= Requests.indexRequest()
					.index("taxi")
					.type("points")
					.source(json);
			requestIndexer.add(rqst);
		}
	}


	private static class simplePointHBaseSink extends RichSinkFunction<Tuple2<String,SimplePointAvro>>
	{
		private Table table=null;
		private int cnt=0;
		private int maxBufferCnt=5;
		private List<Put> puts=null;

		@Override
		public void open(Configuration parameters) throws Exception {
			table=conn.getTable(TableName.valueOf("simplePointTable"));
			puts=new ArrayList<>();
		}

		@Override
		public void invoke(Tuple2<String, SimplePointAvro> value, Context context) throws Exception {
			Put put =new Put(Bytes.toBytes(value.f0));
			SimplePointAvro point=value.f1;
			put.addColumn(Bytes.toBytes("points")
					,Bytes.toBytes( String.valueOf(point.getTrajId()).split("-")[1]+String.valueOf(point.getUtc()))
					,Utils.serializeSimplePoint(point));
			puts.add(put);
			cnt++;
			if (cnt>maxBufferCnt)
			{
				table.put(puts);
				cnt=0;
				puts.clear();
			}
		}

		@Override
		public void close() throws Exception {
			if (puts.size()>0)
			{
				table.put(puts);
			}
			puts=null;
			table.close();
		}
	}
	public static class trajPointHBaseSink extends RichSinkFunction<TrajPointAvro>
	{
		private Table table=null;
		private int cnt=0;
		private int maxBufferCnt=5;
		private List<Put> puts=null;

		@Override
		public void open(Configuration parameters) throws Exception {
			table=conn.getTable(TableName.valueOf("dailyTrajTable"));
			puts=new ArrayList<>();
		}


		@Override
		public void invoke(TrajPointAvro value, Context context) throws Exception {
			String rowkey=Utils.timeStamp2Date(value.getUtc(),"yyyyMMdd")+"-"
					+String.valueOf(value.getTaxiId());
			Put put=new Put(Bytes.toBytes(rowkey));
			put.addColumn(Bytes.toBytes("points")
					,Bytes.toBytes(value.getUtc())
					,Utils.serializeTrajPoint(value));
			puts.add(put);
			cnt++;
			if (cnt>maxBufferCnt)
			{
				table.put(puts);
				puts.clear();
				cnt=0;
			}
		}

		@Override
		public void close() throws Exception {
			if (puts.size()>0)
			{
				table.put(puts);
			}
			puts=null;
			table.close();
		}
	}


	public static void initHBase()
	{
		try {
			conf = HBaseConfiguration.create();
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin();
			//createOrOverwrite(tableName);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	public static void createOrOverwrite(TableName tableName)
	{
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
			else
				createTable(tableName);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	public  static void createTable(TableName tableName) throws IOException
	{
		TableDescriptorBuilder tableDesBuilder=TableDescriptorBuilder.newBuilder(tableName);
		ColumnFamilyDescriptorBuilder columnFamilyDesBuilder=ColumnFamilyDescriptorBuilder
				.newBuilder(Bytes.toBytes("taxiData"));
		tableDesBuilder.setColumnFamily(columnFamilyDesBuilder.build());

		admin.createTable(tableDesBuilder.build());
	}






}
