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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Properties;


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
public class TaxiDataFilterAndSink {
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

		//kafka config
		Properties props=new Properties();
		props.put("bootstrap.servers","localhost:9092");
		props.put("group.id","flinkProcess");
		props.put("auto.offset.reset","earliest");
		props.put("key.deserializer","org.apach.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apach.kafka.common.serialization.StringDeserializer");

		//create consumer
        FlinkKafkaConsumer consumer=new FlinkKafkaConsumer<String>("taxi1"
				,new SimpleStringSchema(),props);
        consumer.setStartFromEarliest();
        consumer.assignTimestampsAndWatermarks(new TSExtractor());
        //stream processing
		//read from kafka and transfer to object

		DataStream<String>messageStream=env.addSource(consumer);

//		DataStream<TrajPoint>trajPointStream=messageStream.map(x->TrajPoint.fromString(x))
//				.assignTimestampsAndWatermarks(new TSExtractor());

		DataStream<TrajPoint>trajPointStream=messageStream.map(x->TrajPoint.fromString(x));
		DataStream<TrajPoint>filtered=trajPointStream.filter(new RangeFilter());
		filtered.print();
		filtered.writeUsingOutputFormat(new HBaseOutputFormat());

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}


	public static class RangeFilter implements FilterFunction<TrajPoint>
	{

		@Override
		public boolean filter(TrajPoint trajPoint) throws Exception {
			double lon=trajPoint.getLon();
			double lat=trajPoint.getLat();
			double speed=trajPoint.getSpeed();
			if (lon >114.3509 && lon<114.3757 && lat>30.5184 && lat <114.3757)
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

	public static class HBaseOutputFormat implements OutputFormat<TrajPoint>
	{

		private BufferedMutator mutator =null;
		private int count=0;


		@Override
		public void configure(Configuration configuration) {
		}

		@Override
		public void open(int i, int i1) throws IOException {


			initHBase();
			TableName tableName=TableName.valueOf("taxiTable");

			BufferedMutatorParams params=new BufferedMutatorParams(tableName);
			params.writeBufferSize(1024*1024);
			mutator=conn.getBufferedMutator(params);
			count=0;
		}

		@Override
		public void writeRecord(TrajPoint trajPoint) throws IOException {

			String cell=GeoHash.withCharacterPrecision(trajPoint.getLat(),trajPoint.getLon(),GEOHASH_PRECISION).toBase32();
			//String date=Utils.timeStamp2Date(trajPoint.getUtc(),null);
			Put put=new Put(Bytes.toBytes(cell+String.valueOf(trajPoint.getUtc())));
			put.addColumn(Bytes.toBytes("taxiData")
					,Bytes.toBytes(trajPoint.getTaxiId())
					,trajPoint.getUtc()
					,Bytes.toBytes(trajPoint.toString()));


			mutator.mutate(put);
			if (count>4)
			{
				mutator.flush();
				count=0;
			}
			count++;

		}

		@Override
		public void close() throws IOException {
			System.out.println("close");
			try
			{
				if (conn!=null) {
					conn.close();
					admin.close();
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}




	}

	public static void initHBase()
	{
		try {
			conf = HBaseConfiguration.create();
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin();
			TableName tableName=TableName.valueOf("taxiTable");
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
