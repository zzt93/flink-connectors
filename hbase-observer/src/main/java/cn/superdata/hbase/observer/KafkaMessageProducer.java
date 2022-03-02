/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.superdata.hbase.observer;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Map;


/**
 * @author ravi.magham
 */
@Slf4j
public final class KafkaMessageProducer implements Closeable {

	private final KafkaProducer<byte[], String> kafkaProducer;
	private final BackpressureRetryPolicy retryPolicy = new BackpressureRetryPolicy();

	public KafkaMessageProducer(final Map<String, Object> kafkaConfiguration) {
		Preconditions.checkNotNull(kafkaConfiguration);
		kafkaConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
		kafkaConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
		this.kafkaProducer = new KafkaProducer<>(kafkaConfiguration);
	}

	public void sendAsync(ProducerRecord<byte[], String> record) {
		boolean retry = true;
		while (retry) {
			try {
				this.kafkaProducer.send(record);
				break;
			} catch (RuntimeException e) {
				retry = retryPolicy.shouldRetry(e);
			} catch (Exception ex) {
				final String errorMsg = String.format("Failed to send the record to kafka topic [%s] ", record.topic());
				log.error("{}", errorMsg, ex);
				break;
			}
		}

	}

	@Override
	public void close() {
		if (this.kafkaProducer != null) {
			this.kafkaProducer.close();
		}
	}
}
