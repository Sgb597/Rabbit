package Pipline.Rabbit;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TramoCalculationFunction 
    extends ProcessWindowFunction<Tuple2<JoinedEvent, String>, Tuple7<String, String, Timestamp, Timestamp, Double, Double, HashMap<String, Double>>, String, GlobalWindow> {
	private static final long serialVersionUID = 1L;

	@Override
    public void process(String key, Context context, Iterable<Tuple2<JoinedEvent, String>> input, Collector<Tuple7<String, String, Timestamp, Timestamp, Double, Double, HashMap<String, Double>>> out) {
        ArrayList<JoinedEvent> windowEvents = new ArrayList<JoinedEvent>();
        Tramo tramo = new Tramo();

        for (Tuple2<JoinedEvent, String> in: input) {
            windowEvents.add(in.f0);
        }
        
        JoinedEvent firstEvent = windowEvents.get(0);
        JoinedEvent lastEvent = windowEvents.get(windowEvents.size() - 1);
        
        HashMap<String, Double> canTramaInicial = new HashMap<String, Double>(firstEvent.getTramoData());
        HashMap<String, Double> canTramaFinal = new HashMap<String, Double>(lastEvent.getTramoData());
        HashMap<String, Double> tramoMap = new HashMap<String, Double>(tramo.getTramoData());
        
        if(!canTramaFinal.isEmpty() && !canTramaInicial.isEmpty()) {
        	for (String key1: canTramaFinal.keySet()) {
    		    Double result = canTramaFinal.get(key1) - canTramaInicial.get(key1);
    		    tramoMap.put(key1, result);
    		}
        }
        
        /*
         * for deltaTime getTime() returns ms in long format so it has been casted to double to avoid truncation
         * deltaTime is in hours and deltaDistance is in kms
         */
        double deltaTime = (double)(lastEvent.getFecha().getTime() - firstEvent.getFecha().getTime())/(1000.0 * 60.0 * 60.0);
        double deltaDistance = (lastEvent.getDistancia() - firstEvent.getDistancia());
        double velocity = deltaDistance/deltaTime;
        
        tramo.setFechaInicio(firstEvent.getFecha());
        tramo.setIdConductor(firstEvent.getIdConductor());
        tramo.setIdVehiculo(firstEvent.getIdVehiculo());
        tramo.setFechaFinal(lastEvent.getFecha());
        tramo.setTramoData(tramoMap);
        tramo.setDistancia(deltaDistance);
        tramo.setVelocidad(velocity);
        
        out.collect(new Tuple7<String, String, Timestamp, Timestamp, Double, Double, HashMap<String, Double>>(
        		tramo.getIdVehiculo(),
        		tramo.getIdConductor(),
        		tramo.getFechaInicio(),
        		tramo.getFechaFinal(),
        		tramo.getDistancia(),
        		tramo.getVelocidad(),
        		tramo.getTramoData()
        		));
    }
}