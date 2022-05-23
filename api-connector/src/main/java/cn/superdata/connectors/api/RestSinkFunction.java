package cn.superdata.connectors.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.SerializableObject;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.*;

@Slf4j
public class RestSinkFunction extends RichSinkFunction<RowData> {

	private static final long serialVersionUID = 1L;
	private static final int CONNECTION_RETRY_DELAY = 500;
	private final SerializableObject lock = new SerializableObject();

	private final String url;
	private final int maxNumRetries;
	private final SerializationSchema<RowData> serializer;
	private volatile boolean isRunning = true;
	private transient CloseableHttpClient client;

	public RestSinkFunction(String url, int maxNumRetries, SerializationSchema<RowData> serializer) {
		this.url = url;
		checkArgument(
				maxNumRetries >= -1,
				"maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");

		this.maxNumRetries = maxNumRetries;
		this.serializer = serializer;
	}

	@Override
	public void close() throws Exception {
		// flag this as not running any more
		isRunning = false;

		// clean up in locked scope, so there is no concurrent change to the stream and client
		synchronized (lock) {
			// we notify first (this statement cannot fail). The notified thread will not continue
			// anyways before it can re-acquire the lock
			lock.notifyAll();

			try {
				if (client != null) {
					client.close();
				}
			} catch (IOException e) {
				log.error("", e);
			}
		}
	}

	private void createConnection() throws IOException {
		client = HttpClients.createDefault();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		createConnection();
	}

	private void post(String json) throws IOException {
		HttpPost httpPost = new HttpPost(url);

		StringEntity entity = new StringEntity(json);
		httpPost.setEntity(entity);
		httpPost.setHeader("Accept", "application/json");
		httpPost.setHeader("Content-type", "application/json");

		CloseableHttpResponse response = client.execute(httpPost);
		log.info("{}", response);
		if (response.getCode() != HttpStatus.SC_OK) {
		}
	}

	@Override
	public void invoke(RowData value, Context context) throws Exception {
		String json = serialize(value);

		try {
			post(json);
		} catch (IOException e) {
			// if no re-tries are enable, fail immediately
			if (maxNumRetries == 0) {
				throw new IOException(
						"Failed to send message '"
								+ value
								+ "' to http server at "
								+ url
								+ ". Connection re-tries are not enabled.",
						e);
			}

			log.error(
					"Failed to send message '"
							+ value
							+ "' to http server at "
							+ url
							+ ". Trying to reconnect...",
					e);

			// do the retries in locked scope, to guard against concurrent close() calls
			// note that the first re-try comes immediately, without a wait!

			synchronized (lock) {
				IOException lastException = null;
				int retries = 0;

				while (isRunning && (maxNumRetries < 0 || retries < maxNumRetries)) {

					// first, clean up the old resources
					try {
						if (client != null) {
							client.close();
						}
					} catch (IOException ee) {
						log.error("Could not close http from failed write attempt", ee);
					}

					// try again
					retries++;

					try {
						// initialize a new connection
						createConnection();

						// re-try the write
						post(json);

						// success!
						return;
					} catch (IOException ee) {
						lastException = ee;
						log.error(
								"Re-connect to http server and send message failed. Retry time(s): "
										+ retries,
								ee);
					}

					// wait before re-attempting to connect
					lock.wait(CONNECTION_RETRY_DELAY);
				}

				// throw an exception if the task is still running, otherwise simply leave the
				// method
				if (isRunning) {
					throw new IOException(
							"Failed to send message '"
									+ value
									+ "' to http server at "
									+ url
									+ ". Failed after "
									+ retries
									+ " retries.",
							lastException);
				}
			}
		}
	}

	private String serialize(RowData value) {
		return new String(serializer.serialize(value));
	}
}