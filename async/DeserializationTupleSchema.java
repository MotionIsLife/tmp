package org.apache.flink.streaming.examples.async.my.async;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.io.IOException;
import java.nio.charset.Charset;

public class DeserializationTupleSchema implements DeserializationSchema<Tuple5<String, String, String, String, String>> {

	private final TupleTypeInfo<Tuple5<String, String, String, String, String>> info;

	public DeserializationTupleSchema() {
		this.info = new TupleTypeInfo<>(new TypeInformation[] {
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO
		});
	}

	@Override
	public Tuple5<String, String, String, String, String> deserialize(byte[] message) throws IOException {
		Any any = JsonIterator.parse(message).readAny();
		String s1 = any.toString("ImageName");
		String s2 = any.toString("CommandLine");
		String s3 = any.toString("B_EventGlobalID");
		String s4 = new String(message, Charset.defaultCharset());
		String s5 = any.toString("B_VendorEventID");
		return null;
	}

	@Override
	public boolean isEndOfStream(Tuple5<String, String, String, String, String> nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Tuple5<String, String, String, String, String>> getProducedType() {
		return this.info;
	}
}
