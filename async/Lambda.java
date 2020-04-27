package org.apache.flink.streaming.examples.async.my.async;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

public class Lambda extends SimpleCondition<Tuple5<String, String, String, String, String>> {

	public Lambda() {
	}

	@Override
	public boolean filter(Tuple5<String, String, String, String, String> stringStringStringStringStringTuple5) throws Exception {
		return false;
	}

}
