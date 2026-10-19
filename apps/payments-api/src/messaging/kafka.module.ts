import { Global, Inject, Module, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, type Producer } from 'kafkajs';


export const KAFKA_PRODUCER = Symbol('KAFKA_PRODUCER');

@Global()
@Module({
    providers: [
        {
            provide: KAFKA_PRODUCER,
            inject: [ConfigService],
            useFactory: (config: ConfigService) => 
                new Kafka({
                    clientId: 'payments-api',
                    brokers: config.getOrThrow<string>('KAFKA_BROKERS').split(','),
                }).producer({
                    idempotent: true,
                    maxInFlightRequests: 1,
                }),
        },
    ],
    exports: [KAFKA_PRODUCER],
})

export class KafkaModule implements OnModuleInit, OnApplicationShutdown {
    constructor(@Inject(KAFKA_PRODUCER) private readonly producer: Producer) {}
    onModuleInit() {
        return this.producer.connect();
    }
    onApplicationShutdown() {
        return this.producer.disconnect();
    }
}