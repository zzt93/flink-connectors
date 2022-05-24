package cn.superdata.connectors.api;

import cn.superdata.connectors.api.config.HttpLookupConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.*;

@Slf4j
public class RestTableSource implements ScanTableSource, LookupTableSource {

	private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
	private final DataType producedDataType;
	private final String url;
	private final int maxNumRetries;

	public RestTableSource(
			String url, DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			DataType producedDataType, int maxNumRetries) {
		this.url = url;
		this.decodingFormat = decodingFormat;
		this.producedDataType = producedDataType;
		this.maxNumRetries = maxNumRetries;
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
				url,
				maxNumRetries, deserializer);

		return SourceFunctionProvider.of(sourceFunction, false);
	}

	@Override
	public DynamicTableSource copy() {
		return new RestTableSource(url, decodingFormat, producedDataType, maxNumRetries);
	}

	@Override
	public String asSummaryString() {
		return "Socket Table Source";
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
				context,
				producedDataType);

		String[] keyNames = new String[context.getKeys().length];
		List<String> fieldNames = getFieldNames(producedDataType);
		for (int i = 0; i < keyNames.length; i++) {
			int[] innerKeyArr = context.getKeys()[i];
			String fieldName = fieldNames.get(innerKeyArr[0]);
			keyNames[i] = fieldName;
		}
		RestSourceLookupFunction.ColumnData columnData = RestSourceLookupFunction.ColumnData.builder().keyNames(keyNames).build();
		RestSourceLookupFunction lookupFunction = new RestSourceLookupFunction(columnData, getHttpLookupOptions(null), deserializer, url);
		return TableFunctionProvider.of(lookupFunction);
	}

	private HttpLookupConfig getHttpLookupOptions(DataType physicalRowDataType) {
//		List<String> fieldNames = getFieldNames(physicalRowDataType);
		return HttpLookupConfig.builder()
				.url(url)
//				.columnNames(fieldNames)
				.build();
	}


	/**
	 * Returns the first-level field names for the provided {@link DataType}.
	 *
	 * <p>Note: This method returns an empty list for every {@link DataType} that is not a composite
	 * type.
	 */
	public static List<String> getFieldNames(DataType dataType) {
		log.info("DataType: {}", dataType);
		final LogicalType type = dataType.getLogicalType();
		if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE) {
			return getFieldNames(dataType.getChildren().get(0));
		} else if (isCompositeType(type)) {
			return LogicalTypeChecks.getFieldNames(type);
		}
		return Collections.emptyList();
	}
}