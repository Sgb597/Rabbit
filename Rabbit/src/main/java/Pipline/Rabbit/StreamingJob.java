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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema; 
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class StreamingJob {
	
   public static ArrayList<String> tramoClassifier(ArrayList<Event> historicalData) {
   	
   	ArrayList<String> tramos = new ArrayList<String>();
   	
   	for(int i = 0; i <  historicalData.size(); i++) {
   		
   		Event estado = historicalData.get(i);
   		Event nextEstado = historicalData.get(i + 1);
   		
   		String currentId = estado.getIdEstado();
   		String nextId = nextEstado.getIdEstado();
   		
   		switch(currentId) {
	   		case "37+":
	   			switch(nextId) {
		   			case "38+": 
		   				tramos.add("PARADO");
		   			case "50+": 
		   				tramos.add("PARADO");
		   			case "243": 
		   				tramos.add("PARADO");
	   			}
	   		
	   		case "38+":
	   			switch(nextId) {
	   			case "37+": 
	   				tramos.add("MOVIMIENTO");
	   			case "50+": 
	   				tramos.add("MOVIMIENTO");
	   			case "243": 
	   				tramos.add("MOVIMIENTO");
				}
	   			
	   		case "50+":
	   			switch(nextId) {
	   			case "37+": 
	   				tramos.add("MOVIMIENTO");
	   			case "38+": 
	   				tramos.add("PARADO");
	   			case "243": 
	   				if( i != 0 ) {
	   					Event lastEstado = historicalData.get(i - 1);
	   					tramos.add(lastEstado.getIdEstado());
	   				} else {
	   					tramos.add("Cambio de conductor -> Cambio de país");
	   				}
				}
	   			
	   		case "243":
	   			switch(nextId) {
	   			case "37+": 
	   				tramos.add("MOVIMIENTO");
	   			case "38+": 
	   				tramos.add("PARADO");
	   			case "50+": 
	   				if( i != 0 ) {
	   					Event lastEstado = historicalData.get(i - 1);
	   					tramos.add(lastEstado.getIdEstado());
	   				} else {
	   					tramos.add("Cambio de conductor -> Cambio de país");
	   				}
				}
   		}
   	}
   	return tramos;
   }
	
	public static void main(String[] args) throws Exception {
		
		// the host, port, and queue name to connect to
        final String hostname = "localhost";
        final String virtualHost = "/";
        final String userName = "guest";
        final String password = "guest";
        final String queueName = "historico";
        final int port = 5672;
        
        // ArrayLists to collect ids for one vehicle and to store the classification of the ids for one vehicle
        ArrayList<Event> historicalData = new ArrayList<Event>();
        ArrayList<String> tramos = new ArrayList<String>();

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
		    
		final DataStream<String> stream = env
		    .addSource(new RMQSource<String>(
		        connectionConfig,           // config for the RabbitMQ connection
		        queueName,               	// name of the RabbitMQ queue to consume
		        false,                    	// use correlation ids; can be false if only at-least-once is required
		        new SimpleStringSchema()	// deserialization schema to turn messages into Java objects
		        ));
		
		stream.flatMap(new FlatMapFunction<String, String>() {
		    @Override
		    public void flatMap(String value, Collector<String> out)
		        throws Exception {
		    	int i = 0;
		        for(String col: value.split(",")){
		        	if( i == 15) {
				    	Event event = new Event();
		        		event.setIdEstado(col.replaceAll("\"", ""));
		        		historicalData.add(event); // if the stream gets to the 15th column in the csv row (where Id Estado is) store it
		        		System.out.println(event.getIdEstado());
		        	}
		        	i++;
		        }
		        System.out.println("Amount of rows to be processed: " + historicalData.size());
				tramos = tramoClassifier(historicalData);
				
				for(String t: tramos) {
					System.out.println(t);
				}
		    }
		});
		
		// execute program
		env.execute("Historical Data Pipeline for one vehicle");
	}
}
