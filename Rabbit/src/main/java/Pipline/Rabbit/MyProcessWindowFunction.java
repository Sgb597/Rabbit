package Pipline.Rabbit;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction 
    extends ProcessWindowFunction<Tuple2<JoinedEvent, String>, Tuple6<String, String, Timestamp, Timestamp, Double, Double>, String, GlobalWindow> {
	private static final long serialVersionUID = 1L;

	@Override
    public void process(String key, Context context, Iterable<Tuple2<JoinedEvent, String>> input, Collector<Tuple6<String, String, Timestamp, Timestamp, Double, Double>> out) {
        ArrayList<JoinedEvent> windowEvents = new ArrayList<JoinedEvent>();
        Double distanciaInicial = 0.0;
        Double distanciaFinal = 0.0;
        Tramo tramo = new Tramo();
        Double [] firstEventFields;
        Double [] lastEventFields;

        for (Tuple2<JoinedEvent, String> in: input) {
            windowEvents.add(in.f0);
        }

        int limit = windowEvents.size();
        int i = 0;
        
        for (JoinedEvent e: windowEvents) {
            if (i == 0) {
            	e.get
            	// Capturar información del evento inicial
                distanciaInicial = e.getDistancia();
                tramo.setFechaInicio(e.getFecha());
                tramo.setIdConductor(e.getIdConductor());
                tramo.setIdVehiculo(e.getIdVehiculo());
            }
            if (i == (limit - 1)) {
            	// Capturar información del evento final
                distanciaFinal = e.getDistancia();
                tramo.setFechaFinal(e.getFecha()); 
            }
            i++;
        }
        
        /*
         * for deltaTime getTime() returns ms in long format so it has been casted to double to avoid truncation
         * deltaTime is in hours and deltaDistance is in kms
         */
        double deltaTime = (double)(tramo.getFechaFinal().getTime() - tramo.getFechaInicio().getTime())/(1000.0 * 60.0 * 60.0);
        double deltaDistance = (distanciaFinal - distanciaInicial)/1000;
        double velocity = deltaDistance/deltaTime;
        
        tramo.setDistancia(deltaDistance);
        tramo.setVelocidad(velocity);
        out.collect(new Tuple6<String, String, Timestamp, Timestamp, Double, Double>(
        		tramo.getIdVehiculo(),
        		tramo.getIdConductor(),
        		tramo.getFechaInicio(),
        		tramo.getFechaFinal(),
        		tramo.getDistancia(),
        		tramo.getVelocidad()
        		));
    }
	
	public static Double[] deltaFields(Double [])
}