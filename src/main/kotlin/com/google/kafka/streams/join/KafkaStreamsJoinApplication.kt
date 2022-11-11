package com.google.kafka.streams.join

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder

@SpringBootApplication
class KafkaStreamsJoinApplication {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val applicationContext = SpringApplicationBuilder(KafkaStreamsJoinApplication::class.java).run(*args)
            applicationContext.registerShutdownHook()
        }
    }

}
