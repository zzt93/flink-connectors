package cn.superdata.connectors.api;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

public class RestDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	// define all options statically
	public static final ConfigOption<String> URL = ConfigOptions.key("url")
			.stringType()
			.noDefaultValue();
	public static final ConfigOption<String> TOKEN = ConfigOptions.key("token")
			.stringType()
			.noDefaultValue();
	public static final ConfigOption<Integer> MAX_RETRY = ConfigOptions.key("max-retry")
			.intType().defaultValue(1);

	@Override
	public String factoryIdentifier() {
		return "rest"; // used for matching to `connector = '...'`
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(URL);
		options.add(FactoryUtil.FORMAT); // use pre-defined option for format
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(TOKEN);
		options.add(MAX_RETRY);
		return options;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		// either implement your custom validation logic here ...
		// or use the provided helper utility
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		// discover a suitable decoding format
		final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
				DeserializationFormatFactory.class,
				FactoryUtil.FORMAT);

		// validate all options
		helper.validate();

		// get the validated options
		final ReadableConfig options = helper.getOptions();
		final String url = options.get(URL);

		// derive the produced data type (excluding computed columns) from the catalog table
		final DataType producedDataType =
				context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

		// create and return dynamic table source
		return new RestTableSource(url, decodingFormat, producedDataType, options.get(MAX_RETRY));
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		// either implement your custom validation logic here ...
		// or use the provided helper utility
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		// discover a suitable decoding format
		final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
				SerializationFormatFactory.class, FactoryUtil.FORMAT);

		// validate all options
		helper.validate();
		final DataType producedDataType =
				context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

		// get the validated options
		final ReadableConfig options = helper.getOptions();
		final String url = options.get(URL);
		return new RestTableSink(url, options.get(MAX_RETRY), encodingFormat, producedDataType);
	}
}
