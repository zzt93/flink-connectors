package cn.superdata.connectors.api;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

public class RestTableSink implements DynamicTableSink {
	private final String url;
	private final int maxRetries;

	public RestTableSink(String url, int maxRetries) {
		this.url = url;
		this.maxRetries = maxRetries;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return requestedMode;
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		return SinkFunctionProvider.of(new RestSinkFunction(url, "", maxRetries));
	}

	@Override
	public DynamicTableSink copy() {
		return new RestTableSink(url, maxRetries);
	}

	@Override
	public String asSummaryString() {
		return "Rest: " + url;
	}
}
