package Pipline.Rabbit;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction 
    extends ProcessWindowFunction<Tuple2<JoinedEvent, String>, Tuple2<Tramo, String>, String, GlobalWindow> {
	private static final long serialVersionUID = 1L;

	@Override
    public void process(String key, Context context, Iterable<Tuple2<JoinedEvent, String>> input, Collector<Tuple2<Tramo, String>> out) {
        ArrayList<JoinedEvent> windowEvents = new ArrayList<JoinedEvent>();
        Tramo tramo = new Tramo();

        for (Tuple2<JoinedEvent, String> in: input) {
            windowEvents.add(in.f0);
        }
        
        JoinedEvent firstEvent = windowEvents.get(0);
        JoinedEvent lastEvent = windowEvents.get(windowEvents.size() - 1);
        
        System.out.println("FIRST AND LAST EVENT MAPS");
        System.out.println(firstEvent.getTramoData());
        System.out.println(firstEvent.getTramoData());
        /*
         * for deltaTime getTime() returns ms in long format so it has been casted to double to avoid truncation
         * deltaTime is in hours and deltaDistance is in kms
         */
        double deltaTime = (double)(lastEvent.getFecha().getTime() - firstEvent.getFecha().getTime())/(1000.0 * 60.0 * 60.0);
        double deltaDistance = (lastEvent.getDistancia() - firstEvent.getDistancia())/1000;
        double velocity = deltaDistance/deltaTime;
        
        tramo.setFechaInicio(firstEvent.getFecha());
        tramo.setIdConductor(firstEvent.getIdConductor());
        tramo.setIdVehiculo(firstEvent.getIdVehiculo());
        tramo.setFechaFinal(lastEvent.getFecha());
        tramo.setTramoData(subtractTramoMaps(lastEvent.getTramoData(), firstEvent.getTramoData()));
        tramo.setDistancia(deltaDistance);
        tramo.setVelocidad(velocity);
        
        out.collect(new Tuple2<Tramo, String>(
        		tramo,
        		tramo.getIdVehiculo()
        		));
    }

	private HashMap<String, Double> subtractTramoMaps(HashMap<String, Double> lastEvent, HashMap<String, Double> firstEvent) {
		HashMap<String, Double> tempMap = new HashMap<String, Double>();
		
		for (String key: lastEvent.keySet()) {
		    Double result = lastEvent.get(key) - firstEvent.get(key);
		    tempMap.put(key, result);
		}
		return tempMap;
	}
}