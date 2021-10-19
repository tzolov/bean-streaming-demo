package com.example.bean.streaming;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;

import static com.example.bean.streaming.PCollectionPrintUtils.printRows;

@SpringBootApplication(exclude = {
		MongoAutoConfiguration.class,
		MongoDataAutoConfiguration.class
})
public class BeanStreamingDemoApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(BeanStreamingDemoApplication.class, args);
	}

	@Override
	public void run(String... args) {

		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
		Pipeline pipeline = Pipeline.create(options);

		//PCollection<Row> kafkaRecordsAsRows = StringPayloadUtils.kafkaRecordsAsRows(
		// pipeline, StringPayloadUtils.stringPayloadSchema);
		PCollection<Row> kafkaRecordsAsRows = CloudEventsPayloadUtils.cloudEventKafkaRecordsAsRows(
				pipeline, CloudEventsPayloadUtils.cloudEventSchema);

		//PCollection<Row> sqlRows = kafkaRecordsAsRows.apply(SqlTransform.query("SELECT * FROM PCOLLECTION"));
		PCollection<Row> sqlRows = kafkaRecordsAsRows.apply(SqlTransform.query(
				"SELECT msg_partition, count(*) as cnt " +
						"FROM PCOLLECTION " +
						"GROUP BY msg_partition, " +
						"TUMBLE(rowtime, INTERVAL '10' SECOND)"));

		PCollectionPrintUtils.printRows(sqlRows);
		PCollectionPrintUtils.rowsToFile("target/part", sqlRows);

		pipeline.run();
	}

	final public static Schema groupByResultSchema = Schema.builder()
			.addInt32Field("msg_partition")
			.addInt64Field("cnt")
			.build();
}
