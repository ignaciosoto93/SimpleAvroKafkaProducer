package com.simpleavrokafka;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class AvroFileToTopic implements AvroFileToTopicInterface {

	private GenericDatumReader<GenericData.Record> datum;
	private File file;
	private DataFileReader<GenericData.Record> reader;
	private Schema schema;
	private GenericData.Record record;
	private Injection<GenericRecord, byte[]> recordInjection;
	private KafkaProducer<String, byte[]> producer;

	public AvroFileToTopic(File file, Properties props) throws IOException {
		this.file=file;
		this.datum = new GenericDatumReader<GenericData.Record>();
		this.reader = new DataFileReader<GenericData.Record>(file, datum);
		this.schema = reader.getSchema();
		this.record = new GenericData.Record(schema);
		this.recordInjection = GenericAvroCodecs.toBinary(schema);
		this.producer = new KafkaProducer<String, byte[]>(props);
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	public DataFileReader<GenericData.Record> getReader() {
		return reader;
	}

	public void setReader(DataFileReader<GenericData.Record> reader) {
		this.reader = reader;
	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public GenericData.Record getRecord() {
		return record;
	}

	public void setRecord(GenericData.Record record) {
		this.record = record;
	}

	public Injection<GenericRecord, byte[]> getRecordInjection() {
		return recordInjection;
	}

	public void setRecordInjection(Injection<GenericRecord, byte[]> recordInjection) {
		this.recordInjection = recordInjection;
	}

	public KafkaProducer<String, byte[]> getProducer() {
		return producer;
	}

	public void setProducer(KafkaProducer<String, byte[]> producer) {
		this.producer = producer;
	}

	public GenericDatumReader<GenericData.Record> getDatum() {
		return datum;
	}

	public void setDatum(GenericDatumReader<GenericData.Record> datum) {
		this.datum = datum;
	}

	public void sendWholeFile(String topicName) throws IOException {
		this.reader = new DataFileReader<GenericData.Record>(file, datum);
		while (reader.hasNext()) {
			reader.next(record);
			byte[] bytes = recordInjection.apply(record);
			ProducerRecord<String, byte[]> theRecord = new ProducerRecord<String, byte[]>(topicName, bytes);
			producer.send(theRecord);
		}
		reader.close();
		producer.close();
	}

}
