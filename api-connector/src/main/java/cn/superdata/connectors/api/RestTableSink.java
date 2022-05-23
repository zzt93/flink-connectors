package cn.superdata.connectors.api;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class RestTableSink implements DynamicTableSink {
	private final String url;
	private final int maxRetries;
	private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
	private final DataType producedDataType;

	public RestTableSink(String url, int maxRetries, EncodingFormat<SerializationSchema<RowData>> encodingFormat, DataType producedDataType) {
		this.url = url;
		this.maxRetries = maxRetries;
		this.encodingFormat = encodingFormat;
		this.producedDataType = producedDataType;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return requestedMode;
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		final SerializationSchema<RowData> serializer = encodingFormat.createRuntimeEncoder(
				context,
				producedDataType);
		return SinkFunctionProvider.of(new RestSinkFunction(url, maxRetries, serializer));
	}

	@Override
	public DynamicTableSink copy() {
		return new RestTableSink(url, maxRetries, encodingFormat, producedDataType);
	}

	@Override
	public String asSummaryString() {
		return "Rest: " + url;
	}
}
