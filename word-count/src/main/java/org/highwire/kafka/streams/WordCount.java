package org.highwire.kafka.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

/**
 * Example taken from https://docs.confluent.io/current/streams/quickstart.html.
 *
 * WordCount is a simple demonstration of a Kafka Streams application.
 * It consumes a stream of String values from a topic, converting them
 * to lowercase, splitting them on non-word characters, and finally
 * emitting a count of words to an output topic table.
 *
 * @see org.apache.kafka.streams.kstream.KStream
 * @see org.apache.kafka.streams.kstream.KTable
 */
public class WordCount {

	/**
	 * properties is a simple utility class that returns a java.util.Properties
	 * configured with various StreamsConfig and ConsumerConfig key/value pairs.
	 *
	 * @param brokers value for StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
	 * @param id value for StreamsConfig.APPLICATION_ID_CONFIG
	 * @param offset value for ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
	 * @return a Properties with the following properties defined:
	 * StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
	 * ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
	 * StreamsConfig.APPLICATION_ID_CONFIG
	 * StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG
	 * StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
	 * @see org.apache.kafka.streams.StreamsConfig
	 * @see org.apache.kafka.clients.consumer.ConsumerConfig
	 */
	public static Properties properties(final String brokers, final String id, final String offset) {
		Properties properties = new Properties();
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, id);
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		return properties;
	}

	/**
 	 * streams initializes a new WordCount KafkaStreams object based on the specified properties,
	 * and input / output topics.  The stream must be started and closed by the caller.
	 *
	 * @param properties the KafkaStreams properties to use, at a
	 * minimum it needs StreamsConfig.BOOTSTRAP_SERVERS_CONFIG and
	 * StreamsConfig.APPLICATION_ID_CONFIG to be set.
	 * @param inTopic the stream topic to read words from
	 * @param outTopic the table to write word counts to
	 * @see properties
	 * @see org.apache.kafka.streams.KafkaStreams
	 */
	public static KafkaStreams streams(final Properties properties, final String inTopic, final String outTopic) {
		final KStreamBuilder builder = new KStreamBuilder();

		// read input from inputTopic stream
		final KStream<String,String> inputStream = builder.stream(inTopic);

		// convert the input values into a count of "words"
		final KTable<String,Long> wordCounts = inputStream 
			.mapValues( value -> value.toLowerCase() )			// lowercase values
			.flatMapValues( value -> Arrays.asList(value.split("\\W+")) )	// split on non-word chars
			.selectKey( (key, value) -> value )				// map values to keys
			.groupByKey()							// group keys together
			.count("Counts");						// count keys

		// write output to outputTopic table
		wordCounts.to(Serdes.String(), Serdes.Long(), outTopic);

		// configure and return the stream object
		return new KafkaStreams(builder, properties);
	}
}
