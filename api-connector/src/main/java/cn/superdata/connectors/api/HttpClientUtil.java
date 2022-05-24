package cn.superdata.connectors.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.IOException;

@Slf4j
public class HttpClientUtil {
	public static CloseableHttpResponse post(CloseableHttpClient client, String url, String body) throws IOException {
		HttpPost httpPost = new HttpPost(url);

		StringEntity entity = new StringEntity(body);
		httpPost.setEntity(entity);
		httpPost.setHeader("Accept", "application/json");
		httpPost.setHeader("Content-type", "application/json");

		log.info("{}", httpPost);
		CloseableHttpResponse response = client.execute(httpPost);
//		log.info("{}", new BufferedReader(new InputStreamReader(response.getEntity().getContent())).lines().collect(Collectors.joining("\n")));
		if (response.getCode() != HttpStatus.SC_OK) {
		}
		return response;
	}

	public static CloseableHttpClient createClient() throws IOException {
		return HttpClients.createDefault();
	}
}
