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

package Pipline.Rabbit;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

public class StreamingJob {
   
    public static void main(String[] args) throws Exception {
       
        // the host, port, and queue name to connect to
        final String hostname = "localhost";
        final String virtualHost = "/";
        final String userName = "guest";
        final String password = "guest";
        final String inputQueue = "joinedstream";
        final int port = 5672;

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      
      // checkpointing is required for exactly-once or at-least-once guarantees
      env.enableCheckpointing(1000);  

      final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
          .setHost(hostname)
          .setPort(port)
          .setVirtualHost(virtualHost)
          .setUserName(userName)
          .setPassword(password)
          .build();
      
        final DataStream<String> textStream = env
        	    .addSource(new RMQSource<String>(
        	        connectionConfig,
        	        inputQueue,
        	        false,
        	        new SimpleStringSchema()));	
        
        DataStream<Tuple2<JoinedEvent, String>> parsedStream = textStream
                .flatMap(new Parser());
        
        DataStream<Tuple7<String, String, Timestamp, Timestamp, Double, Double, HashMap<String, Double>>> tramoStream = parsedStream
        		.keyBy(value -> value.f1)
		        .window(GlobalWindows.create())
		        .trigger(new TramoTrigger())
		        .process(new MyProcessWindowFunction());
        
	  tramoStream
	 .map(new MapFunction<Tuple7<String, String, Timestamp, Timestamp, Double, Double, HashMap<String, Double>>, Object>() {
	     @Override
	     public Object map(Tuple7<String, String, Timestamp, Timestamp, Double, Double, HashMap<String, Double>>summary) throws Exception {
	         System.out.println(summary.f0);
	         System.out.println(summary.f1);
	         System.out.println(summary.f2);
	         System.out.println(summary.f3);
	         System.out.println(summary.f4);
	         System.out.println(summary.f5);
	         System.out.println(summary.f6);
	         return null;
	     }
	 });
        
        CassandraSink.addSink(tramoStream)
        .setQuery("INSERT INTO tfm.tramos(idVehiculo , IdConductor , FechaInicio , FechaFinal , Distancia , Velocidad, tramoData) values (?, ?, ? , ?, ?, ?, ?);")
        .setHost("127.0.0.1")
        .build();
        
        env.execute("Joined Evento + Tramo Creation");
    }
   
    public static final class Parser implements FlatMapFunction<String, Tuple2<JoinedEvent, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<JoinedEvent, String>> out) throws Exception {
            JoinedEvent event = new JoinedEvent(value);
            out.collect(new Tuple2<JoinedEvent, String>(event, event.getIdVehiculo()));
        }
    }
}