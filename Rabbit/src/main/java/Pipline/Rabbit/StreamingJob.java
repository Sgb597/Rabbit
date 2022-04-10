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
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.asm7.org.objectweb.asm.tree.analysis.Value;
import org.apache.flink.util.Collector;

public class StreamingJob {
	
	public static void main(String[] args) throws Exception {
		
		// the host, port, and queue name to connect to
        final String hostname = "localhost";
        final String virtualHost = "/";
        final String userName = "guest";
        final String password = "guest";
        final String queueName = "historico";
        final int port = 5672;
        
        final String input = "/home/sebastian/Documents/TFM/data/historico_one_vehicle_NH.csv";

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		
//		// checkpointing is required for exactly-once or at-least-once guarantees
//		env.enableCheckpointing(1000);	
//
//		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
//		    .setHost(hostname)
//		    .setPort(port)
//		    .setVirtualHost(virtualHost)
//		    .setUserName(userName)
//		    .setPassword(password)
//		    .build();
		
		final DataStreamSource<String> textStream = env.readTextFile(input);
		
		final DataStream<String> stream = textStream
				.name("text_stream");
		stream.flatMap(new Parser());
		stream.print();
		
		final DataStream<Tuple2<Event, Integer>> eventStream = stream
				.name("event_stream");
				.window(GlobalWindows.create());
		
		// execute program
		env.execute("Streaming Job with Windows");
	}
	
	public static final class Parser implements FlatMapFunction<String, Event> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Event> out) throws Exception {
			int i = 0;
            Event event = new Event();
	        for( String col: value.split(",") ){
                switch(i){
                    case 2: 
                    	
                    	try {
	                		SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
							// Cast String Date to Date 
							Date date = dateParser.parse(col.replaceAll("\"", ""));
							event.setFecha(date);
                    	} catch(ParseException e) {
                    		e.printStackTrace();
                    	}
						break;

					case 4:
						try {
							// Cast String Id to Integer
							String idVehiculo = col.replaceAll("\"", "");
							event.setIdVehiculo(idVehiculo);
						} catch(Exception e) {
                    		e.printStackTrace();
                    	}
						break;

					case 5:
						try {
							// Cast String Id to Integer
							String idConductor = col.replaceAll("\"", "");
							event.setIdConductor(idConductor);
						} catch(Exception e) {
                    		e.printStackTrace();
                    	}
						break;

					case 7:
						try {
							// Cast String latitude to Double
							String cleanLat = col.replaceAll("\"", "");
							double latitud = Double.parseDouble(cleanLat.replace(",", "."));
							event.setLatitud(latitud);
						} catch(ClassCastException e) {
                    		e.printStackTrace();
                    	}
						break;

					case 8:
						try {
							// Cast String longitud to Double
							String cleanLong = col.replaceAll("\"", "");
							double longitud = Double.parseDouble(cleanLong.replace(",", "."));
							event.setLongitud(longitud);
						} catch(ClassCastException e) {
                    		e.printStackTrace();
                    	}
						break;

					case 13:
						try {
							event.setIdEstado(col.replaceAll("\"", ""));
						} catch(ClassCastException e) {
                    		e.printStackTrace();
                    	}
						break;
                }
	        	i++;
	        	out.collect(event);
	        }
		}
	}
}
	
		
