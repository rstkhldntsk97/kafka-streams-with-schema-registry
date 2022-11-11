package com.google.kafka.streams.join

import com.google.entity.Event
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class TestConsumer {

    @Test
    fun testConsumer() {
        val props = Properties()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ConsumerConfig.GROUP_ID_CONFIG] = "testConsumer"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = Serdes.String().deserializer().javaClass
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = getCanonicalSerde<Event>().deserializer().javaClass
        props[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = "http://localhost:8081"

        val consumer = KafkaConsumer<String, Event>(props)
        consumer.subscribe(listOf("outboxTopic"))
        while (true) {
            val poll = consumer.poll(Duration.ofMillis(100))
            if (!poll.isEmpty) {
                assertEquals("event participant", poll.first().value().eventName)
                break
            }
        }
    }

    private inline fun <reified T : SpecificRecord?> getCanonicalSerde(): SpecificAvroSerde<T> {
        return SpecificAvroSerde<T>().also {
            it.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"), false)
        }
    }

}