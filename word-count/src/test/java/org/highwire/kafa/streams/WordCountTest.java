package org.highwire.kafka.streams;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;

import kafka.zk.EmbeddedZookeeper;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

public class WordCountTest {

	// id contains the application id, which must be unique amoung applications
	private static final String id = "WordCountTest";

	// inTopic is the name of the topic we push phrases to
	private static final String inTopic = "WordCountTestIn";

	// outTopic is the name of the topic containing the word counts
	private static final String outTopic = "WordCountTestOut";

	// offset is the default offset for the kafka consumer to use
	private static final String offset = "earliest";

	// cluster  is a stand-alone kafka broker for testing
	@ClassRule
	public static final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(1);

	// zkServer is a stand-alone zookeeper server for testing
	public static final EmbeddedZookeeper zkServer = new EmbeddedZookeeper();

	@BeforeClass
	public static void startKafkaCluster() throws Exception {
		try {
			cluster.createTopic(inTopic);
		} catch (TopicExistsException e) {
			/* ignorable */
		}
		try {
			cluster.createTopic(outTopic);
		} catch (TopicExistsException e) {
			/* ignorable */
		}
	}

	@Test
	public void wordCountTest() throws Exception {
		// lines contains our input phrases
		final List<String> lines = Arrays.asList(
			"Hello, World!",
			"Welcome to this unit test.",
			"We expect it to count words.",
			"All your words are belong to us!",
			"Words, are, to all, you us!"
		);

		// actual holds the actual results, to be compared to expected
		List<KeyValue<String, Long>> actual = null;

		// expected holds the expected results, to be compared to actual
		final List<KeyValue<String, Long>> expected = Arrays.asList(
			new KeyValue<>("all", 2L),
			new KeyValue<>("are", 2L),
			new KeyValue<>("belong", 1L),
			new KeyValue<>("count", 1L),
			new KeyValue<>("expect", 1L),
			new KeyValue<>("hello", 1L),
			new KeyValue<>("it", 1L),
			new KeyValue<>("test", 1L),
			new KeyValue<>("this", 1L),
			new KeyValue<>("to", 4L),
			new KeyValue<>("unit", 1L),
			new KeyValue<>("us", 2L),
			new KeyValue<>("we", 1L),
			new KeyValue<>("welcome", 1L),
			new KeyValue<>("words", 3L),
			new KeyValue<>("world", 1L),
			new KeyValue<>("you", 1L),
			new KeyValue<>("your", 1L)
		);

		// zkConnect is the zookeeper address for our EmbeddedZookeeper
		final String zkConnect = "127.0.0.1:" + zkServer.port();

		// initialize properties used by the streams processer
		Properties properties = WordCount.properties(cluster.bootstrapServers(), id, offset);
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
		properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
		properties.put("zookeeper.connect", zkConnect);

		// clear the test broker
		 IntegrationTestUtils.purgeLocalStreamsState(properties);

		// initialize the streams processor
		KafkaStreams streams = WordCount.streams(properties, inTopic, outTopic);
		try {
			// cleanup any old state
			streams.cleanUp();

			// start the WordCount client
			streams.start();
	
			// produce data on the input topic
			Properties producerConfig = new Properties();
	    		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
			producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	
			IntegrationTestUtils.produceValuesSynchronously(inTopic, lines, producerConfig, new SystemTime());
	
			// verify the output data
			Properties consumerConfig = new Properties();
			consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
			consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, id + "_consumer");
			consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);
			consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
	
			actual = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outTopic, expected.size());
		}
		finally {
			try {
				streams.close();
			} finally {
				streams.cleanUp();
			}
		}

		assertThat(actual, notNullValue());
		assertThat(actual.size(), equalTo(expected.size()));
		assertThat(actual, containsInAnyOrder(expected.toArray()));
	}
}
