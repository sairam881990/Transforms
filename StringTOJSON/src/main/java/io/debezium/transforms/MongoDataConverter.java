package io.debezium.transforms;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.apache.kafka.connect.data.Schema;

public class MongoDataConverter {
	static SchemaBuilder builder = SchemaBuilder.struct();

	private static final Logger log = LoggerFactory.getLogger(MongoDataConverter.class);

	public static Struct convertRecord(Entry<String, BsonValue> keyvalueforStruct, Schema schema, Struct struct) {

		convertFieldValue(keyvalueforStruct, struct, schema);
		return struct;
	}

	public static void convertFieldValue(Entry<String, BsonValue> keyvalueforStruct, Struct struct, Schema schema) {
		Object colValue = null;
		String key = keyvalueforStruct.getKey();
		BsonType type = keyvalueforStruct.getValue().getBsonType();

		switch (type) {

		case NULL: {
			colValue = null;
			break;
		}

		case STRING: {
			colValue = keyvalueforStruct.getValue().asString().getValue().toString();
			break;
		}

		case OBJECT_ID: {
			colValue = keyvalueforStruct.getValue().asObjectId().getValue().toString();
			break;
		}

		case DOUBLE: {
			colValue = keyvalueforStruct.getValue().asDouble().getValue();
			break;
		}

		case BINARY: {
			colValue = keyvalueforStruct.getValue().asBinary().getData();
			break;
		}

		case INT32: {
			colValue = keyvalueforStruct.getValue().asInt32().getValue();
			break;
		}

		case INT64: {
			colValue = keyvalueforStruct.getValue().asInt64().getValue();
			break;
		}

		case BOOLEAN: {
			colValue = keyvalueforStruct.getValue().asBoolean().getValue();
			break;
		}

		case DATE_TIME: {

			colValue = keyvalueforStruct.getValue().asDateTime().getValue();
			break;
		}

		case JAVASCRIPT: {

			colValue = keyvalueforStruct.getValue().asJavaScript().getCode();
			break;
		}

		case JAVASCRIPT_WITH_SCOPE: {

			Struct struct1 = new Struct(schema.field(keyvalueforStruct.getKey()).schema());
			Struct struct2 = new Struct(schema.field(keyvalueforStruct.getKey()).schema().field("scope").schema());

			struct1.put("code", keyvalueforStruct.getValue().asJavaScriptWithScope().getCode());

			BsonDocument doc = keyvalueforStruct.getValue().asJavaScriptWithScope().getScope().asDocument();

			for (Entry<String, BsonValue> entry4 : doc.entrySet()) {
				convertFieldValue(entry4, struct2, schema.field(keyvalueforStruct.getKey()).schema());
			}

			struct1.put("scope", struct2);

			colValue = struct1;
			break;
		}

		case REGULAR_EXPRESSION: {

			Struct struct1 = new Struct(schema.field(keyvalueforStruct.getKey()).schema());
			struct1.put("regex", keyvalueforStruct.getValue().asRegularExpression().getPattern());
			struct1.put("options", keyvalueforStruct.getValue().asRegularExpression().getOptions());

			colValue = struct1;
			break;
		}

		case TIMESTAMP: {
			colValue = keyvalueforStruct.getValue().asTimestamp().getTime();
			break;
		}

		case DECIMAL128: {
			colValue = keyvalueforStruct.getValue().asDecimal128().getValue().toString();
			break;
		}

		case DOCUMENT: {

			Schema schema1 = schema.field(keyvalueforStruct.getKey()).schema();

			Struct struct1 = new Struct(schema1);
			BsonDocument doc = keyvalueforStruct.getValue().asDocument();

			for (Entry<String, BsonValue> entry4 : doc.entrySet()) {
				convertFieldValue(entry4, struct1, schema1);
			}

			colValue = struct1;
			break;
		}

		case ARRAY: {

			if (keyvalueforStruct.getValue().asArray().isEmpty()) {

				System.out.println("entered loop");
				List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

				ArrayList<String> list = new ArrayList<>();

				for (BsonValue value : arrValues) {
					String temp = value.asString().getValue().toString();
					list.add(temp);
				}
				colValue = list;
				break;
			}

			else {
				BsonType valueType = keyvalueforStruct.getValue().asArray().get(0).getBsonType();

				switch (valueType) {

				case NULL: {

					System.out.println("entered null");
					colValue = null;
					break;
				}

				case STRING: {

					List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

					ArrayList<String> list = new ArrayList<>();

					for (BsonValue value : arrValues) {
						String temp = value.asString().getValue();
						list.add(temp);
					}
					colValue = list;
					break;
				}

				case JAVASCRIPT: {

					List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

					ArrayList<String> list = new ArrayList<>();

					for (BsonValue value : arrValues) {
						String temp = value.asJavaScript().getCode();
						list.add(temp);
					}
					colValue = list;
					break;
				}

				case OBJECT_ID: {

					List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

					ArrayList<String> list = new ArrayList<>();

					for (BsonValue value : arrValues) {

						String temp = value.asObjectId().getValue().toString();
						list.add(temp);
					}
					colValue = list;
					break;
				}

				case DOUBLE: {

					List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

					ArrayList<Double> list = new ArrayList<>();

					for (BsonValue value : arrValues) {
						double temp = value.asDouble().getValue();
						list.add(temp);

					}
					colValue = list;
					break;
				}

				case BINARY: {

					List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

					ArrayList<byte[]> list = new ArrayList<>();

					for (BsonValue value : arrValues) {
						byte[] temp = value.asBinary().getData();
						list.add(temp);

					}
					colValue = list;
					break;
				}

				case INT32: {

					List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

					ArrayList<Integer> list = new ArrayList<>();

					for (BsonValue value : arrValues) {
						int temp = value.asInt32().getValue();
						list.add(temp);

					}
					colValue = list;
					break;
				}

				case INT64:
				case DATE_TIME: {
					List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

					ArrayList<Long> list = new ArrayList<>();

					for (BsonValue value : arrValues) {
						long temp = value.asInt64().getValue();
						list.add(temp);

					}
					colValue = list;
					break;
				}

				case DECIMAL128: {
					List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

					ArrayList<String> list = new ArrayList<>();

					for (BsonValue value : arrValues) {
						String temp = value.asDecimal128().getValue().toString();
						list.add(temp);

					}
					colValue = list;
					break;
				}

				case TIMESTAMP: {
					List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

					ArrayList<Integer> list = new ArrayList<>();

					for (BsonValue value : arrValues) {
						int temp = value.asInt32().getValue();
						list.add(temp);

					}
					colValue = list;
					break;
				}

				case BOOLEAN: {

					List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

					ArrayList<Boolean> list = new ArrayList<>();

					for (BsonValue value : arrValues) {
						boolean temp = value.asBoolean().getValue();
						list.add(temp);

					}
					colValue = list;
					break;
				}

				case DOCUMENT: {

					List<BsonValue> arrValues = keyvalueforStruct.getValue().asArray().getValues();

					Schema schema1 = schema.field(keyvalueforStruct.getKey()).schema().valueSchema();

					Iterator<BsonValue> valIterator = arrValues.iterator();

					ArrayList<Struct> doclist = new ArrayList<Struct>();

					while (valIterator.hasNext()) {
						Struct struct1 = new Struct(schema1);
						for (Entry<String, BsonValue> entry9 : valIterator.next().asDocument().entrySet()) {
							convertFieldValue(entry9, struct1, schema1);
						}
						doclist.add(struct1);
					}
					colValue = doclist;

					break;
				}

				default:
					break;
				}
			}

			break;
		}
		default:
			break;
		}

		struct.put(key, keyvalueforStruct.getValue().isNull() ? null : colValue);
	}

	public static void addFieldSchema(Entry<String, BsonValue> keyValuesforSchema, SchemaBuilder builder) {

		String key = keyValuesforSchema.getKey();
		BsonType type = keyValuesforSchema.getValue().getBsonType();

		switch (type) {

		case NULL: {

			log.warn("Data type {} not currently supported", type);
			break;
		}

		case STRING:
		case JAVASCRIPT: {
			builder.field(key, Schema.STRING_SCHEMA);
			break;
		}

		case OBJECT_ID: {
			builder.field(key, Schema.STRING_SCHEMA);
			break;
		}

		case DOUBLE: {
			builder.field(key, Schema.FLOAT64_SCHEMA);
			break;
		}

		case BINARY: {
			builder.field(key, Schema.BYTES_SCHEMA);
			break;
		}

		case INT32:
		/* case MIN_KEY: */ {
			builder.field(key, Schema.INT32_SCHEMA);
			break;
		}

		case INT64: {
			builder.field(key, Schema.INT64_SCHEMA);
			break;
		}

		case BOOLEAN: {
			builder.field(key, Schema.BOOLEAN_SCHEMA);
			break;
		}

		case DATE_TIME: {
			builder.field(key, Schema.INT64_SCHEMA);
			break;
		}

		case TIMESTAMP: {
			builder.field(key, Schema.INT32_SCHEMA);
			break;
		}

		case DECIMAL128: {
			builder.field(key, Schema.STRING_SCHEMA);
			break;
		}

		case JAVASCRIPT_WITH_SCOPE: {

			SchemaBuilder jswithscope = SchemaBuilder.struct();
			jswithscope.field("code", Schema.STRING_SCHEMA);
			SchemaBuilder scope = SchemaBuilder.struct();
			BsonDocument doc = keyValuesforSchema.getValue().asJavaScriptWithScope().getScope().asDocument();

			for (Entry<String, BsonValue> entry4 : doc.entrySet()) {
				addFieldSchema(entry4, scope);
			}

			Schema build = scope.build();
			jswithscope.field("scope", build).build();
			builder.field(key, jswithscope);
			break;

		}

		case REGULAR_EXPRESSION: {
			SchemaBuilder regexwop = SchemaBuilder.struct();

			regexwop.field("regex", Schema.STRING_SCHEMA);
			regexwop.field("options", Schema.STRING_SCHEMA);
			builder.field(key, regexwop.build());
			break;

		}

		case DOCUMENT: {

			SchemaBuilder builderDoc = SchemaBuilder.struct();
			BsonDocument doc = keyValuesforSchema.getValue().asDocument();

			for (Entry<String, BsonValue> entry4 : doc.entrySet()) {
				addFieldSchema(entry4, builderDoc);
			}
			builder.field(key, builderDoc.build());
			break;
		}

		case ARRAY: {

			if (keyValuesforSchema.getValue().asArray().isEmpty()) {
				builder.field(key, SchemaBuilder.array(Schema.STRING_SCHEMA).build());
				break;
			}

			else {

				BsonType valueType = keyValuesforSchema.getValue().asArray().get(0).getBsonType();

				switch (valueType) {

				case NULL: {
					log.warn("Data type {} not currently supported", valueType);
					break;
				}

				case STRING:
				case JAVASCRIPT: {
					builder.field(key, SchemaBuilder.array(Schema.STRING_SCHEMA).build());
					break;
				}

				case OBJECT_ID: {
					builder.field(key, SchemaBuilder.array(Schema.STRING_SCHEMA).build());
					break;
				}

				case DOUBLE: {
					builder.field(key, SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build());
					break;
				}

				case BINARY: {
					builder.field(key, SchemaBuilder.array(Schema.BYTES_SCHEMA).build());
					break;
				}

				case INT32: {
					builder.field(key, SchemaBuilder.array(Schema.INT32_SCHEMA).build());
					break;
				}

				case INT64: {
					builder.field(key, SchemaBuilder.array(Schema.INT64_SCHEMA).build());
					break;
				}

				case BOOLEAN: {
					builder.field(key, SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build());
					break;
				}

				case DATE_TIME: {
					builder.field(key, SchemaBuilder.array(Schema.INT64_SCHEMA).build());
					break;
				}

				case TIMESTAMP: {
					builder.field(key, SchemaBuilder.array(Schema.INT32_SCHEMA).build());
					break;
				}

				case DECIMAL128: {
					builder.field(key, SchemaBuilder.array(Schema.STRING_SCHEMA).build());
					break;
				}

				case DOCUMENT: {

					SchemaBuilder builderDoc = SchemaBuilder.struct();
					BsonDocument doc = keyValuesforSchema.getValue().asArray().get(0).asDocument();

					for (Entry<String, BsonValue> entry3 : doc.entrySet()) {
						addFieldSchema(entry3, builderDoc);
					}
					Schema build = builderDoc.build();
					builder.field(key, SchemaBuilder.array(build).build());
					break;
				}

				default:
					break;
				}
				break;
			}

		}
		default:
			break;
		}
	}
}
