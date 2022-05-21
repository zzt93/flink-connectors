package cn.superdata.connectors.api;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

public class RestSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

	private final String url;
	private final String token;
	private final DeserializationSchema<RowData> deserializer;

	private volatile boolean isRunning = true;

	public RestSourceFunction(String url, String token, DeserializationSchema<RowData> deserializer) {
		this.url = url;
		this.token = token;
		this.deserializer = deserializer;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return deserializer.getProducedType();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
	}

	@Override
	public void run(SourceContext<RowData> ctx) throws Exception {
		while (isRunning) {

			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
		try {
		} catch (Throwable t) {
			// ignore
		}
	}
}