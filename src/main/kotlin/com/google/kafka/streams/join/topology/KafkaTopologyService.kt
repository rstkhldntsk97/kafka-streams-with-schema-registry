package com.google.kafka.streams.join.topology

import com.google.entity.Event
import com.google.entity.ParticipantEvent
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.ValueJoiner
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.Collections

@Service
class KafkaTopologyService {

    private val log: Logger = LoggerFactory.getLogger(KafkaTopologyService::class.java.name)

    private val valueJoiner: ValueJoiner<Event, ParticipantEvent, Event> = ValueJoiner { event: Event, participantEvent: ParticipantEvent ->
        log.info("StreamJoining $event and $participantEvent")
        event.eventName = "${event.eventName} ${participantEvent.eventName}"
        event
    }

    @Autowired
    fun buildTopology(streamsBuilder: StreamsBuilder) {
        val inboxStream = streamsBuilder.stream("participantEvent", Consumed.with(Serdes.String(), getCanonicalSerde<ParticipantEvent>()))
            .peek { k, v -> log.info("Topic: participantEvent received key: $k, value: $v") }

        streamsBuilder.stream("event", Consumed.with(Serdes.String(), getCanonicalSerde<Event>()))
            .peek { k, v -> log.info("Topic: events received key: $k, value: $v") }
            .join(inboxStream, valueJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(2)))
            .peek { _, v -> log.info("Sending to outboxTopic ${v.eventName}") }
            .to("outboxTopic", Produced.with(Serdes.String(), getCanonicalSerde<Event>()))
    }

    private inline fun <reified T : SpecificRecord?> getCanonicalSerde(): SpecificAvroSerde<T> {
        return SpecificAvroSerde<T>().also {
            it.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"), false)
        }
    }

}