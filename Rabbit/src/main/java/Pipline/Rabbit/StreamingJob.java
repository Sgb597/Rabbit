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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
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
    	final String hostname = "10.204.0.7";
        final String virtualHost = "/";
        final String userName = "admin";
        final String password = "admin";
        final String historicoQueue = "historico";
	    final String cantramaQueue = "cantrama";
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
      
//        final DataStream<String> textStream = env
//        	    .addSource(new RMQSource<String>(
//        	        connectionConfig,
//        	        inputQueue,
//        	        false,
//        	        new SimpleStringSchema()));	

        final DataStream<String> historicoTextStream = env
        	    .addSource(new RMQSource<String>(
        	    	connectionConfig,
        	        historicoQueue,
        	        false,
        	        new SimpleStringSchema()));
        
        final DataStream<String> cantramaTextStream = env
        	    .addSource(new RMQSource<String>(
        	    	connectionConfig,
        	        cantramaQueue,
        	        false,
        	        new SimpleStringSchema()));
        
        DataStream<Tuple2<Event, String>> eventStream = historicoTextStream
                .flatMap(new EventoParser());
        
        DataStream<Tuple2<Cantrama, String>> cantramaStream = cantramaTextStream
                .flatMap(new CantramaParser());
        
        DataStream<String> joinStream = eventStream.join(cantramaStream)
	        .where(value -> value.f1)
	        .equalTo(value -> value.f1)
	        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1900)))
	        .apply (new JoinFunction<Tuple2<Event, String>, Tuple2<Cantrama, String>, String> (){

				private static final long serialVersionUID = 1L;

				@Override
	            public String join(Tuple2<Event, String> event, Tuple2<Cantrama, String> cantrama) {
	            	String output = "";
//	            	Boolean joinStream = compareDates(event.f0.getFecha() ,cantrama.f0.getFecha());
//	            	
//	            	if (joinStream) {
//	            		output = outputEventString(event.f0) + "," + outputCantramaString(cantrama.f0);
//	            		System.out.println("JOINED EVENT " + output);
//	            	} else {
//	            		output = outputEventString(event.f0);
//	            		System.out.println("NON-JOINED EVENT " + output);
//	            	}
	            	output = outputEventString(event.f0) + "," + outputCantramaString(cantrama.f0);
	            	return output;
	            }
	        });
        
        DataStream<Tuple2<JoinedEvent, String>> parsedStream = joinStream
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
	         System.out.println("HISTORICO DATA " + summary.f0 + ", " + summary.f1 + ", " +  summary.f2.toString() + ", " +  summary.f3.toString() + ", " +  summary.f4.toString() + ", " +  summary.f5.toString());
	         System.out.println("CANTRAMA DATA " +summary.f6);
	         return null;
	     }
	 });
        
        CassandraSink.addSink(tramoStream)
        .setQuery("INSERT INTO tfm.tramos(idVehiculo , IdConductor , FechaInicio , FechaFinal , Distancia , Velocidad, tramoData) values (?, ?, ? , ?, ?, ?, ?);")
        .setHost("127.0.0.1")
        .build();
        
        env.execute("JOINED EVENT + TRAMO + CASS");
    }
   
    public static final class Parser implements FlatMapFunction<String, Tuple2<JoinedEvent, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<JoinedEvent, String>> out) throws Exception {
            JoinedEvent event = new JoinedEvent(value);
            out.collect(new Tuple2<JoinedEvent, String>(event, event.getIdVehiculo()));
        }
    }
    
    public static final class EventoParser implements FlatMapFunction<String, Tuple2<Event, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<Event, String>> out) throws Exception {
            Event event = new Event(value);
            out.collect(new Tuple2<Event, String>(event, event.getIdVehiculo()));
        }
    }
    
    public static final class CantramaParser implements FlatMapFunction<String, Tuple2<Cantrama, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<Cantrama, String>> out) throws Exception {
            Cantrama cantrama = new Cantrama(value);
            out.collect(new Tuple2<Cantrama, String>(cantrama, cantrama.getIdVehiculo()));
        }
    }
   
   public static boolean compareDates(Timestamp eventstamp, Timestamp cantramaStamp) {
   	
	   SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	   
	   String cantramaParsedDate = dateParser.format(cantramaStamp);
	   String eventParsedDate = dateParser.format(eventstamp);
	          	
   	if (cantramaParsedDate.equals(eventParsedDate)) {
   		return true;
   	}
	return false;
   }
   
   public static String outputCantramaString(Cantrama cantrama) {
		String attributes = "";
		
		attributes = cantrama.getCruiseActive() + ","
				+ cantrama.getRpmExcesivas() + ","
				+ cantrama.getFrenadasBruscas() + ","
				+ cantrama.getAceleracionesBruscas() + ","
				+ cantrama.getcNoPredictiva2() + ","
				+ cantrama.getzRoja2() + ","
				+ cantrama.getzMasVerde2() + ","
				+ cantrama.getFrenadasBruscas2() + ","
				+ cantrama.getAceleracionesBruscas2() + ","
				+ cantrama.getRalInec2() + ","
				+ cantrama.getTiempoConduccionCrucero2() + ","
				+ cantrama.getMetrosAscendidos2() + ","
				+ cantrama.getMetrosDescendidos2() + ","
				+ cantrama.getOdometro2() + ","
				+ cantrama.getTotalFuel2() + ","
				+ cantrama.getTiempoRal2() + ","
				+ cantrama.getConsumoRal2() + ","
				+ cantrama.getTiempoConduccion2() + ","
				+ cantrama.getnFreno3() + ","
				+ cantrama.getnEmbrague3() + ","
				+ cantrama.getTiempoMotor3();
		
		return attributes;
	}
   
	public static String outputEventString(Event event) {
		String attributes = "";
		
		attributes = event.getIdVehiculo() + ","
				+ event.getIdConductor() + ","
				+ event.getIdEstado() + ","
				+ event.getFecha().toString() + ","
				+ event.getDistancia().toString();
		
		return attributes;
	}
}