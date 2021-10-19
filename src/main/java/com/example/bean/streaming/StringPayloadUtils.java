package com.example.bean.streaming;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTime;

/**
 * @author Christian Tzolov
 */
public class StringPayloadUtils {

	// String Payload

	final public static Schema stringPayloadSchema = Schema.builder()
			.addDateTimeField("rowtime")
			//.addInt64Field("rowtime")
			.addInt32Field("msg_partition")
			.addStringField("msg_key")
			.addStringField("msg_value")
			.build();

	public static PCollection<Row> kafkaRecordsAsRows(Pipeline pipeline, Schema schema) {
		String kafkaBootstrapServer = "localhost:9092";
		//String kafkaTopic = "user-titles";
		String kafkaTopic = "my-cloud-events";

		PCollection<KafkaRecord<String, String>> kafkaRecords = pipeline
				.apply(KafkaIO.<String, String>read()
						.withBootstrapServers(kafkaBootstrapServer)
						.withTopic(kafkaTopic)  // use withTopics(List<String>) to read from multiple topics.
						//.withKeyDeserializer(LongDeserializer.class)
						.withKeyDeserializer(StringDeserializer.class)
						.withValueDeserializer(StringDeserializer.class)
				);

		PCollection<Row> rows = kafkaRecords
				.apply(ParDo.of(new StringStringToRowDoFn(schema))).setRowSchema(schema);

		return rows;
	}

	public static class StringStringToRowDoFn extends DoFn<KafkaRecord<String, String>, Row> {

		private final Schema schema;

		public StringStringToRowDoFn(Schema schema) {
			this.schema = schema;
		}

		@ProcessElement
		public void processElement(ProcessContext ctx) {
			// Get the current record instance
			KafkaRecord<String, String> record = ctx.element();

			// Create a Row with the appSchema schema and values from the current POJO
			Row row = Row.withSchema(this.schema)
					.addValues(
							new DateTime(record.getTimestamp()),
							//new Date(record.getTimestamp()),
							record.getPartition(),
							record.getKV().getKey(),
							record.getKV().getValue())
					.build();

			// Output the Row representing the current POJO
			ctx.output(row);
		}
	}
}
