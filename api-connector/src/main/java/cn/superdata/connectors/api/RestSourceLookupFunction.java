package cn.superdata.connectors.api;

import cn.superdata.connectors.api.config.HttpLookupConfig;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.rest.messages.json.RawJsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class RestSourceLookupFunction extends TableFunction<RowData> {

	@Getter
	private final ColumnData columnData;

	@Getter private final HttpLookupConfig options;
	private final DeserializationSchema<RowData> deserializer;
	private final String url;

	private transient Gauge<Integer> httpCallCounter;
	private transient AtomicInteger localHttpCallCounter;
	private transient CloseableHttpClient client;

	@Builder
	public RestSourceLookupFunction(
			ColumnData columnData,
			HttpLookupConfig options, DeserializationSchema<RowData> deserializer, String url) {

		this.columnData = columnData;
		this.options = options;
		this.deserializer = deserializer;
		this.url = url;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		this.localHttpCallCounter = new AtomicInteger(0);
		this.client = HttpClientUtil.createClient();
		this.httpCallCounter =
				context
						.getMetricGroup()
						.gauge("http-table-lookup-call-counter", () -> localHttpCallCounter.intValue());
	}

	/** This is a lookup method which is called by Flink framework in a runtime. */
	public void eval(Object... values) {
		RowData result = lookupByKeys(values);
		collect(result);
	}

	@SneakyThrows
	public RowData lookupByKeys(Object[] values) {
		RowData keyRow = GenericRowData.of(values);
		log.info("Used Keys - {}, {}", Arrays.toString(values), keyRow);

		HashMap<String, Object> m = new HashMap<>();
		for (int i = 0; i < values.length; i++) {
			m.put(columnData.getKeyNames()[i], values[i]);
		}

		localHttpCallCounter.incrementAndGet();

		String s = new ObjectMapper().writeValueAsString(m);
		try {
			CloseableHttpResponse response = HttpClientUtil.post(client, url, s);
			String collect = new BufferedReader(new InputStreamReader(response.getEntity().getContent())).lines().collect(Collectors.joining("\n"));
			log.info("query result: {}", collect);
			return deserializer.deserialize(collect.getBytes());
		} catch (IOException e) {
			log.error("", e);
		}
		return new GenericRowData(values.length);
	}

	@Data
	@Builder
	@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
	public static class ColumnData implements Serializable {
		private final String[] keyNames;
	}
}
