package com.google.kafka.streams.join.configuration

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration


@Configuration
@EnableKafka
@EnableKafkaStreams
class KafkaConfiguration {

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfigs(properties: KafkaProperties): KafkaStreamsConfiguration {
        val properties1 = properties.buildStreamsProperties()
        properties1[DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        properties1[DEFAULT_VALUE_SERDE_CLASS_CONFIG] = SpecificAvroSerde::class.java
        properties1[KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG] = true
        properties1[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = "http://localhost:8081"
        return KafkaStreamsConfiguration(properties1)
    }

}