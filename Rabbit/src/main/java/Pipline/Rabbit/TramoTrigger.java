package Pipline.Rabbit;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class TramoTrigger<T, W extends Window> extends Trigger<T, W> {
    private static final long serialVersionUID = 1L;

    private final ValueStateDescriptor<Tuple2<Event, String>> stateDesc = new ValueStateDescriptor<Tuple2<Event, String>>("last-element", TypeInformation.of(new TypeHint<Tuple2<Event, String>>() {}));
    
    public TramoTrigger() {}

    @Override
    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
    	
        ValueState<Tuple2<Event, String>> lastElementState = ctx.getPartitionedState(stateDesc);
        
        Tuple2<Event, String> currentEvent = (Tuple2<Event, String>) element;
        String idEstado = currentEvent.f0.getIdEstado();
        
        if (lastElementState.value() == null) {
        	lastElementState.update((Tuple2<Event, String>) element);
            return TriggerResult.CONTINUE;
        }
        else if (lastElementState.value() != null) {
            lastElementState.update((Tuple2<Event, String>) element);
            if (idEstado.equals("37+") || idEstado.equals("38+")) {
            	lastElementState.update(null);
            	System.out.println("TRIGGER FIRE PARADA");
            	return TriggerResult.FIRE;
            } else if (idEstado.equals("50+")) {
            	lastElementState.update(null);
            	System.out.println("TRIGGER FIRE CAMBIO DE CONDUCTOR");
            	return TriggerResult.FIRE;
            } else if (idEstado.equals("243")) {
	        	lastElementState.update(null);
	        	System.out.println("TRIGGER FIRE CAMBIO DE PAÍS");
	        	return TriggerResult.FIRE;
	        }
        } else {
            return TriggerResult.CONTINUE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

	@Override
	public void clear(W window, TriggerContext ctx) throws Exception {
		// TODO Auto-generated method stub
		
	}

//    @Override
//    public void clear(W window, TriggerContext ctx) throws Exception {
//        ctx.getPartitionedState(stateDesc).clear();
//    }
//
//    public static <T, W extends Window> TramoTrigger<T, W> of(DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
//        return new TramoTrigger<>(deltaFunction, stateSerializer);
//    }
}
