package org.apache.flink.streaming.examples.async.my.async;

import com.jsoniter.any.Any;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CepWithAsyncFunctionJob {

	private static final Pattern<Tuple5<String, String, String, String, String>, Tuple5<String, String, String, String, String>> PATTERN;
	private static final PatternProcessFunction<Tuple5<String, String, String, String, String>, Any> PATTERN_PROCESS_FUNCTION;
	private static final DeserializationSchema<Tuple5<String, String, String, String, String>> DESERIALIZATION_SCHEMA;

	static {
		PATTERN = new PatternHolder().getPattern();
		PATTERN_PROCESS_FUNCTION = new TuplePutternProcessFunction();
		DESERIALIZATION_SCHEMA = new DeserializationTupleSchema();
	}

	public static void main(String[] args) {
		final ParameterTool params = ParameterTool.fromArgs(args);
		String kafkaSourceTopic = params.get("kafka.source.topic");
		String brokersSource = params.get("bootstrap.servers.source", "localhost:9092");
		String brokersSink = params.get("bootstrap.servers.sink", "localhost:9092");
		String kafkaSinkTopic = params.get("kafka.sink.topic");
		String groupId = params.get("groupId", "defaultGroupId");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", brokersSource);
		properties.setProperty("group.id", "test");
		DataStream<Tuple5<String, String, String, String, String>> stream = env
			.addSource(new FlinkKafkaConsumer<>("topic", DESERIALIZATION_SCHEMA, properties));

		SingleOutputStreamOperator<Any> process = CEP
			.pattern(stream, PATTERN)
			.process(PATTERN_PROCESS_FUNCTION);

		AsyncDataStream.unorderedWait(process, null, 10000L, TimeUnit.MILLISECONDS,20);

	}
}
