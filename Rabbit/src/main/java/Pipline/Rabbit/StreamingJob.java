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
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class StreamingJob {
   
    public static void main(String[] args) throws Exception {
       
        // the host, port, and queue name to connect to
        final String hostname = "localhost";
        final String virtualHost = "/";
        final String userName = "guest";
        final String password = "guest";
        final String inputQueue = "toflink";
        final String outputQueue = "fromflink";
        final int port = 5672;
        final String input = "/home/sebastian/Documents/TFM/data/EventosHistorico_muestra_NH.csv";

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
      
//		DataStreamSource<String> textStream = env.readTextFile(input);
 
        DataStream<Tuple2<Event, String>> parsedStream = textStream
                .flatMap(new Parser());
        
        parsedStream
        .map(new MapFunction<Tuple2<Event, String>, Object>() {

			@Override
            public Object map(Tuple2<Event, String>summary) throws Exception {
                System.out.println("Event : "
                        + " IdConductor : " + summary.f0.getIdConductor()
                        + ", IdVehiculo : " + summary.f0.getIdVehiculo()
                        + ", Estado : " + summary.f0.getIdEstado()
                        + ", Fecha : " + summary.f0.getFecha()
                        + ", Distancia : " + summary.f0.getDistancia()
                        );
                return null;
            }
        });    		
        
        DataStream<Tuple6<String, String, Timestamp, Timestamp, Double, Double>> tramoStream = parsedStream
        		.keyBy(value -> value.f1)
		        .window(GlobalWindows.create())
		        .trigger(new TramoTrigger())
		        .process(new MyProcessWindowFunction());
        
        /*
         *  The Output String has the following IdVehiculo + IdConductor + FechaInicio + FechaFinal + Distancia + Velocidad 
         */
//        DataStream<Tuple6<String, String, Date, Date, Double, Double>> outputStream = tramoStream
//        		.map(new MapFunction<Tuple6<String, String, Date, Date, Double, Double>, Tuple6<String, String, Date, Date, Double, Double>>() {
//                    @Override
//                    public Tuple6<String,String,Date,Date,Double,Double> map(Tuple6<String, String, Date, Date, Double, Double>summary) throws Exception {
//                    	SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//                        String inicioAsString = summary.f2.toString();
//                        String finalAsString = summary.f3.toString();
//                        System.out.println("Final Cast of Fecha Inicio " + inicioAsString);
//                        System.out.println("Final Cast of Fecha Final " + finalAsString);
//                        
//                        Date fechaInicio = dateParser.parse(inicioAsString);
//                        Date fechaFinal = dateParser.parse(finalAsString);
//                    	
//                        return new Tuple6<String, String, Date, Date, Double, Double>(
//                				summary.f0,
//                				summary.f1,
//                				fechaInicio,
//                				fechaFinal,
//                				summary.f4,
//                				summary.f5
//                				);
//                    }
//                });
        
        //Pretty Print
        tramoStream
                .map(new MapFunction<Tuple6<String, String, Timestamp, Timestamp, Double, Double>, Object>() {
                    @Override
                    public Object map(Tuple6<String, String, Timestamp, Timestamp, Double, Double>summary) throws Exception {
                        System.out.println("Tramo : "
                                + " IdVehiculo : " + summary.f0
                                + ", IdConductor : " + summary.f1
                                + ", FechaInicio : " + summary.f2
                        		+ ", FechaFinal : " + summary.f3
                        		+ ", Distancia : " + summary.f4
                        		+ ", Velocidad : " + summary.f5);
                        return null;
                    }
                });
        
        
        CassandraSink.addSink(tramoStream)
        .setQuery("INSERT INTO tfm.tramos(idVehiculo , IdConductor , FechaInicio , FechaFinal , Distancia , Velocidad) values (?, ? , ?, ?, ?, ?);")
        .setHost("127.0.0.1")
        .build();
        
        env.execute("Flink + Cassandra");
    }
   
    public static final class Parser implements FlatMapFunction<String, Tuple2<Event, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<Event, String>> out) throws Exception {
            Event event = new Event(value);
            out.collect(new Tuple2<Event, String>(event, event.getIdVehiculo()));
        }
    }
}