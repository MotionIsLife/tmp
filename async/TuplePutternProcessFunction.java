package org.apache.flink.streaming.examples.async.my.async;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;
import com.jsoniter.any.Any;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TuplePutternProcessFunction extends PatternProcessFunction<Tuple5<String, String, String, String, String>, Any> {

	@Override
	public void processMatch(Map<String, List<Tuple5<String, String, String, String, String>>> match, Context ctx, Collector<Any> out) throws Exception {
		Map<Object, Object> collect = match
			.entrySet()
			.stream()
			.map(el -> new AbstractMap.SimpleEntry(el.getKey(), el.getValue()
				.stream()
				.map(el2 -> {
					return "[" + "Rule" + "]" + (String) el2.getField(1);
				}).collect(Collectors.toList()))).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		Any result = Any.wrap(collect);
		out.collect(result);
	}
}
