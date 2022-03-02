package cn.superdata.hbase.observer;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

import static java.util.stream.Collectors.*;

@Slf4j
public class CdcCoprocessor implements RegionCoprocessor {
	public static final String KAFKA_PREFIX_KEY = "kafka.";
	private static final Gson gson = new Gson();
	private static volatile KafkaMessageProducer producer;
	private static volatile String topic;

	@Override
	public Optional<RegionObserver> getRegionObserver() {
		return Optional.of(new CdcRegionObserver(producer));
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		Configuration configuration = env.getConfiguration();

		Map<String, Object> kafkaProperties = configuration.getValByRegex(KAFKA_PREFIX_KEY)
				.entrySet()
				.stream()
				.collect(toMap(e -> e.getKey().substring(KAFKA_PREFIX_KEY.length()), Map.Entry::getValue));
		log.info("start CdcCoprocessor {}", kafkaProperties);
		producer = new KafkaMessageProducer(kafkaProperties);
		topic = configuration.get("topic");
	}

	private static class CdcRegionObserver implements RegionObserver {
		private final KafkaMessageProducer producer;
		public CdcRegionObserver(KafkaMessageProducer producer) {
			this.producer = producer;
			log.info("init CdcRegionObserver");
		}

		@Override
		public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
			log.info("{}, {}, {}, {}", c.getEnvironment(), put.toString(), edit, durability);
			NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
			log.info("{}, {}, {}", familyCellMap.keySet().stream().map(String::new).collect(joining(",")), familyCellMap, put.getAttributesMap());
			producer.sendAsync(new ProducerRecord<>(topic, gson.toJson(familyCellMap)));
		}

		@Override
		public void postDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) throws IOException {
			log.info("{}, {}, {}, {}", c, delete, edit, durability);
			NavigableMap<byte[], List<Cell>> familyCellMap = delete.getFamilyCellMap();
			log.info("{}, {}", familyCellMap, delete.getAttributesMap());
			producer.sendAsync(new ProducerRecord<>(topic, gson.toJson(familyCellMap)));
		}
	}
}


