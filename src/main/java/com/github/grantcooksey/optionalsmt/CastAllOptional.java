package com.github.grantcooksey.optionalsmt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class CastAllOptional<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CastAllOptional.class);

    @Override
    public R apply(R record) {
        LOGGER.trace("CastAllOptional Smt received record: {}", record);
        return applyWithSchema(record);
    }

    @Override
    public ConfigDef config() {
        LOGGER.info("CastAllOptional Smt is getting configured");
        return new ConfigDef();
    }

    @Override
    public void close() {
        LOGGER.info("CastAllOptional Smt is trying to close");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        int count = configs.size();
        LOGGER.info("CastAllOptional Smt found {} configs", count);
    }

    private R applyWithSchema(R record) {
        Schema valueSchema = operatingSchema(record);
        Schema updatedSchema = getOrBuildSchema(valueSchema);

        // Casting within a struct
        final Struct value = Requirements.requireStruct(operatingValue(record), "Set everything to optional");

        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object fieldValue = value.get(field);
            updatedValue.put(updatedSchema.field(field.name()), fieldValue);
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema getOrBuildSchema(Schema valueSchema) {
        Schema updatedSchema;
        final SchemaBuilder builder;

        builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
        for (Field field : valueSchema.fields()) {
            SchemaBuilder fieldBuilder = SchemaBuilder.type(field.schema().type());

            fieldBuilder.optional();  // This is the meat of the SMT. All fields are cast as optional!

            builder.field(field.name(), fieldBuilder.build());
        }

        if (valueSchema.isOptional()) {
            builder.optional();
        }

        updatedSchema = builder.build();
        return updatedSchema;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static final class Value<R extends ConnectRecord<R>> extends CastAllOptional<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
