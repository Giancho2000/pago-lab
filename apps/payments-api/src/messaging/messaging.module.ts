import { Module } from '@nestjs/common';
import { OutboxRelay } from './outbox-relay.js';

@Module({ providers: [OutboxRelay] })
export class MessagingModule {}