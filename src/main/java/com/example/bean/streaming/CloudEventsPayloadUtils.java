package com.example.bean.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.protobuf.ProtobufFormat;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
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
public class CloudEventsPayloadUtils {

	// CloudEvent Payload
	final public static Schema cloudEventSchema = Schema.builder()
			.addDateTimeField("rowtime")
			.addInt32Field("msg_partition")
			.addStringField("msg_key")
			.addStringField("msg_value")
			.addStringField("ce_id")
			.addStringField("ce_source")
			.addStringField("ce_type")
			.addStringField("ce_data")
			.addStringField("ce_time")
			.build();

	/** Beam {@link org.apache.beam.sdk.coders.Coder} to define how to encode and decode values of
	 * type {@link CloudEvent} into byte streams.
	 * Uses the {@link ProtobufFormat} to serialize/deserialize {@link CloudEvent} to/from {@link io.cloudevents.v1.proto.CloudEvent}.
	 * Delegates the {@link ProtoCoder} to encode decode the {@link io.cloudevents.v1.proto.CloudEvent} */
	public static class CloudEventCoder extends CustomCoder<CloudEvent> {

		private transient ProtobufFormat protoFormat;
		private final ProtoCoder<io.cloudevents.v1.proto.CloudEvent> protoCoder;

		public CloudEventCoder() {
			this.protoCoder = ProtoCoder.of(io.cloudevents.v1.proto.CloudEvent.class);
		}

		@Override
		public void encode(CloudEvent cloudEvent, OutputStream outStream) throws IOException {
			io.cloudevents.v1.proto.CloudEvent ce =
					io.cloudevents.v1.proto.CloudEvent.parseFrom(this.getProtoFormat().serialize(cloudEvent));
			this.protoCoder.encode(ce, outStream);
		}

		@Override
		public CloudEvent decode(InputStream inStream) throws IOException {
			io.cloudevents.v1.proto.CloudEvent ce = this.protoCoder.decode(inStream);
			return this.getProtoFormat().deserialize(ce.toByteArray());
		}

		private ProtobufFormat getProtoFormat() {
			if (this.protoFormat == null) {
				this.protoFormat = new ProtobufFormat();
			}
			return this.protoFormat;
		}
	}

	public static PCollection<Row> cloudEventKafkaRecordsAsRows(Pipeline pipeline, Schema schema) {
		String kafkaBootstrapServer = "localhost:9092";
		//String kafkaTopic = "user-titles";
		String kafkaTopic = "my-cloud-events";
		PCollection<KafkaRecord<String, CloudEvent>> kafkaRecords = pipeline
				.apply(KafkaIO.<String, CloudEvent>read()
						.withBootstrapServers(kafkaBootstrapServer)
						.withTopic(kafkaTopic)  // use withTopics(List<String>) to read from multiple topics.
						.withKeyDeserializer(StringDeserializer.class)
						.withValueDeserializerAndCoder(CloudEventDeserializer.class, new CloudEventCoder())
				);

		return kafkaRecords.apply(ParDo.of(new CloudEventToRowDoFn(schema))).setRowSchema(schema);
	}


	public static class CloudEventToRowDoFn extends DoFn<KafkaRecord<String, CloudEvent>, Row> {

		private final Schema schema;

		public CloudEventToRowDoFn(Schema schema) {
			this.schema = schema;
		}

		@ProcessElement
		public void processElement(ProcessContext ctx) {
			// Get the current record instance
			KafkaRecord<String, CloudEvent> record = ctx.element();
			// Create a Row with the appSchema schema and values from the current POJO
			Row row = Row.withSchema(this.schema)
					.addValues(
							new DateTime(record.getTimestamp()),
							record.getPartition(),
							record.getKV().getKey(),
							(record.getKV().getValue() == null)? "null" : record.getKV().getValue().toString(),
							record.getKV().getValue().getId(),
							record.getKV().getValue().getSource().toString(),
							record.getKV().getValue().getType(),
							(record.getKV().getValue().getData() == null)? "null" : new String(record.getKV().getValue().getData().toBytes()),
							(record.getKV().getValue().getTime() == null)? "null" : record.getKV().getValue().getTime().toString()
					)
					.build();

			// Output the Row representing the current POJO
			ctx.output(row);
		}
	}
}
