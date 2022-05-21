package cn.superdata.connectors.api;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class RestDynamicTableSource implements ScanTableSource {

	private final String hostname;
	private final int port;
	private final byte byteDelimiter;
	private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
	private final DataType producedDataType;

	public RestDynamicTableSource(
			String hostname,
			int port,
			byte byteDelimiter,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			DataType producedDataType) {
		this.hostname = hostname;
		this.port = port;
		this.byteDelimiter = byteDelimiter;
		this.decodingFormat = decodingFormat;
		this.producedDataType = producedDataType;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		// in our example the format decides about the changelog mode
		// but it could also be the source itself
		return decodingFormat.getChangelogMode();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

		// create runtime classes that are shipped to the cluster

		final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
				runtimeProviderContext,
				producedDataType);

		final SourceFunction<RowData> sourceFunction = new RestSourceFunction(
				hostname, ""
				, deserializer);

		return SourceFunctionProvider.of(sourceFunction, false);
	}

	@Override
	public DynamicTableSource copy() {
		return new RestDynamicTableSource(hostname, port, byteDelimiter, decodingFormat, producedDataType);
	}

	@Override
	public String asSummaryString() {
		return "Socket Table Source";
	}
}