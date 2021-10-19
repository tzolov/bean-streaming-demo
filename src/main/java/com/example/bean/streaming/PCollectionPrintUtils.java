package com.example.bean.streaming;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

/**
 * @author Christian Tzolov
 */
public class PCollectionPrintUtils {

	public static void rowsToFile(String outputsPrefix, PCollection<Row> rows) {

		Duration WINDOW_TIME = Duration.standardMinutes(1);
		Duration ALLOWED_LATENESS = Duration.standardMinutes(1);

		rows.apply("apply window", Window.<Row>into(FixedWindows.of(WINDOW_TIME))
						.withAllowedLateness(ALLOWED_LATENESS)
						.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
						.accumulatingFiredPanes()
				)
				.apply(ToString.elements())
				.apply("Write results", TextIO.write()
						.withWindowedWrites()
						.withNumShards(1)
						.to(outputsPrefix));
	}

	public static void recordsToFile(String outputsPrefix, PCollection<KafkaRecord<String, String>> kafkaRecords) {
		Duration WINDOW_TIME = Duration.standardMinutes(1);
		Duration ALLOWED_LATENESS = Duration.standardMinutes(1);

		kafkaRecords
				.apply("apply window", Window.<KafkaRecord<String, String>>into(FixedWindows.of(WINDOW_TIME))
						.withAllowedLateness(ALLOWED_LATENESS)
						.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
						.accumulatingFiredPanes()
				)
				.apply("Covert Record to String", MapElements.into(TypeDescriptors.strings())
						.via((KafkaRecord<String, String> record) ->
								record.getTopic() + " : " + record.getTimestamp() + " : " + record.getKV().getKey() + " : " + record.getKV().getValue()))
				//.apply(ToString.elements())
				.apply("Write results", TextIO.write()
						.withWindowedWrites()
						.withNumShards(1)
						.to(outputsPrefix));
	}

	public static PCollection<Row> printRows(PCollection<Row> rows) {
		PCollection<Row> sameRows = rows.apply("print row values: ",
						MapElements.via(
								new SimpleFunction<Row, Row>() {
									@Override
									public Row apply(Row input) {
										if (input != null) {
											System.out.println("PCOLLECTION: " + input.getValues());
										}
										return input;
									}
								}))
				.setRowSchema(rows.getSchema());
		return sameRows;
	}

}
