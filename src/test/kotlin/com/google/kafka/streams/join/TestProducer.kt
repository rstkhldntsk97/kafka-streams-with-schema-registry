package com.google.kafka.streams.join

import com.google.entity.Event
import com.google.entity.ParticipantEvent
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Test
import java.util.*

class TestProducer {

    @Test
    fun testProducer() {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "io.confluent.kafka.serializers.KafkaAvroSerializer"
        props[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = "http://localhost:8081"

        val producer = KafkaProducer<String, ParticipantEvent>(props)
        producer.send(ProducerRecord("participantEvent", "key", ParticipantEvent("participant")))
        producer.close()

        val eventProducer = KafkaProducer<String, Event>(props)
        eventProducer.send(ProducerRecord("event", "key", Event("event")))
        eventProducer.close()
    }

}