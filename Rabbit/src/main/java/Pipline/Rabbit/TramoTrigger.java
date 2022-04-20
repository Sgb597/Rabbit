package Pipline.Rabbit;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
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

    private final DeltaFunction<T> deltaFunction;
    private final ValueStateDescriptor<T> stateDesc;

    private TramoTrigger(DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
        this.deltaFunction = deltaFunction;
        stateDesc = new ValueStateDescriptor<>("last-element", stateSerializer);
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ValueState<T> lastElementState = ctx.getPartitionedState(stateDesc);
        System.out.println(lastElementState.value());
        if (lastElementState.value() == null) {
            lastElementState.update(element);
            return TriggerResult.CONTINUE;
        }
        else if (lastElementState.value() != null) {
            lastElementState.update(element);
            return TriggerResult.FIRE;
        } else {
            return TriggerResult.CONTINUE;
        }
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
        ctx.getPartitionedState(stateDesc).clear();
    }

    public static <T, W extends Window> TramoTrigger<T, W> of(DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
        return new TramoTrigger<>(deltaFunction, stateSerializer);
    }
}
