package Pipline.Rabbit;

import java.util.ArrayList;
import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction 
    extends ProcessWindowFunction<Tuple2<Event, String>, Tuple6<String, String, Date, Date, Double, Double>, String, TimeWindow> {
	private static final long serialVersionUID = 1L;

	@Override
    public void process(String key, Context context, Iterable<Tuple2<Event, String>> input, Collector<Tuple6<String, String, Date, Date, Double, Double>> out) {
        ArrayList<Event> windowEvents = new ArrayList<Event>();
        Double distanciaInicial = 0.0;
        Double distanciaFinal = 0.0;
        Tramo tramo = new Tramo();
        
//        TimeWindow window = context.window();
//        long start = window.getStart();
//        long end = window.getEnd();
//        System.out.println("Window start Time " + start + " End " + end);

        for (Tuple2<Event, String> in: input) {
            windowEvents.add(in.f0);
        }

        int limit = windowEvents.size();
        int i = 0;
        for (Event e: windowEvents) {
            if (i == 0) {
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
        out.collect(new Tuple6<String, String, Date, Date, Double, Double>(
        		tramo.getIdConductor(),
        		tramo.getIdVehiculo(),
        		tramo.getFechaInicio(),
        		tramo.getFechaFinal(),
        		tramo.getDistancia(),
        		tramo.getVelocidad()
        		));
    }
}