package io.debezium.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class StringToJson<R extends ConnectRecord<R>> implements Transformation<R> {

	public R apply(R r) {

		SchemaBuilder schemabuilder = SchemaBuilder.struct();

		BsonDocument val = BsonDocument.parse(r.value().toString());
		System.out.println(val);

		Set<Entry<String, BsonValue>> keyValues = val.entrySet();

		for (Entry<String, BsonValue> keyValuesforSchema : keyValues) {

			MongoDataConverter.addFieldSchema(keyValuesforSchema, schemabuilder);
		}

		Schema finalSchema = schemabuilder.build();
		Struct finalStruct = new Struct(finalSchema);

		for (Entry<String, BsonValue> keyvalueforStruct : keyValues) {

			MongoDataConverter.convertRecord(keyvalueforStruct, finalSchema, finalStruct);
		}
		System.out.println(finalStruct);

		return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), finalSchema, finalStruct,r.timestamp());
	}

	public ConfigDef config() {
		return new ConfigDef();
	}

	public void close() {
	}

	public void configure(Map<String, ?> map) {
	}

}

