package org.apache.flink.streaming.examples.async.my.async;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;

public class PatternHolder {

	private final Pattern<Tuple5<String, String, String, String, String>, Tuple5<String, String, String, String, String>> pattern;
	private final Lambda lambda = new Lambda();

	public PatternHolder() {
		this.pattern = Pattern
			.<Tuple5<String, String, String, String, String>>begin("head", AfterMatchSkipStrategy.noSkip())
			.where(this.lambda);
	}

	public final Pattern<Tuple5<String, String, String, String, String>, Tuple5<String, String, String, String, String>> getPattern() {
		return this.pattern;
	}

}
