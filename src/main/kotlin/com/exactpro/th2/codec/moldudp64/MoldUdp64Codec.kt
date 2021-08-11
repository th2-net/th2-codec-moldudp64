/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec.moldudp64

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.moldudp64.MoldUdp64CodecFactory.Companion.PROTOCOL
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.message.messageType

typealias MessageName = String
typealias MessageType = String

class MoldUdp64Codec(private val settings: MoldUdp64CodecSettings) : IPipelineCodec {
    override fun encode(messageGroup: MessageGroup): MessageGroup {
        val messages = messageGroup.messagesList

        if (messages.isEmpty()) return messageGroup

        require(messages[0].hasMessage()) { "Header message must be a parsed message" }

        val header = with(messages[0].message) {
            val protocol = metadata.protocol
            require(protocol.isEmpty() || protocol == PROTOCOL) { "Unexpected protocol: $protocol (expected: $PROTOCOL)" }
            require(messageType == HEADER_MESSAGE_TYPE) { "Unexpected header message type: $messageType (expected: $HEADER_MESSAGE_TYPE)" }
        }

        val payload = messages.subList(1, messages.size).run {
            require(all(AnyMessage::hasRawMessage)) { "All payload messages must be raw messages" }
            map { it.rawMessage }
        }

        val builder = MessageGroup.newBuilder()

        // encode header and payload here

        return builder.build()
    }

    override fun decode(messageGroup: MessageGroup): MessageGroup {
        val messages = messageGroup.messagesList

        if (messages.isEmpty()) return messageGroup

        require(messages.size == 1) { "Message group contains more than 1 message" }
        require(messages[0].hasRawMessage()) { "Input message is not a raw message" }

        val message = messages[0].rawMessage
        val protocol = message.metadata.protocol

        require(protocol.isEmpty() || protocol == PROTOCOL) { "Unexpected protocol: $protocol (expected: $PROTOCOL)" }

        val builder = MessageGroup.newBuilder()

        // decode message's body here

        return builder.build()
    }

    companion object {
        const val HEADER_MESSAGE_TYPE = "Header"
    }
}
