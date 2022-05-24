package cn.superdata.connectors.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

@Slf4j
public class RestSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

	private static final long serialVersionUID = 1L;
	private static final int CONNECTION_RETRY_DELAY = 500;
	private final String url;
	private final int maxNumRetries;
	private final DeserializationSchema<RowData> deserializer;
	private transient CloseableHttpClient client;

	private volatile boolean isRunning = true;

	public RestSourceFunction(String url, int maxNumRetries, DeserializationSchema<RowData> deserializer) {
		this.url = url;
		this.maxNumRetries = maxNumRetries;
		this.deserializer = deserializer;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return deserializer.getProducedType();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
//		deserializer.open();
		client = HttpClientUtil.createClient();
	}

	@Override
	public void run(SourceContext<RowData> ctx) throws Exception {
		long attempt = 0;
		while (isRunning) {
			try {
				CloseableHttpResponse response = HttpClientUtil.post(client, url, "{}");
				String buffer = new BufferedReader(new InputStreamReader(response.getEntity().getContent())).lines().collect(Collectors.joining("\n"));
				ctx.collect(deserializer.deserialize(buffer.getBytes()));
			} catch (IOException | UnsupportedOperationException e) {
				// if we dropped out of this loop due to an EOF, sleep and retry
				if (isRunning) {
					attempt++;
					if (maxNumRetries == -1 || attempt < maxNumRetries) {
						log.warn(
								"Lost connection to server socket. Retrying in "
										+ CONNECTION_RETRY_DELAY
										+ " msecs...");
						Thread.sleep(CONNECTION_RETRY_DELAY);
					} else {
						// this should probably be here, but some examples expect simple exists of the
						// stream source
						// throw new EOFException("Reached end of stream and reconnects are not
						// enabled.");
						break;
					}
				}
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
		try {
			client.close();
		} catch (Throwable t) {
			// ignore
		}
	}
}