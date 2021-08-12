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

package com.exactpro.th2.codec.moldudp64;

import static com.exactpro.th2.codec.moldudp64.MoldUdp64CodecFactory.PROTOCOL;

import java.util.Objects;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.codec.api.IPipelineCodec;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageGroup;

public class MoldUdp64Codec implements IPipelineCodec {
    public static final String HEADER_MESSAGE_TYPE = "Header";
    public static final String SESSION_FIELD = "Session";
    public static final String SEQUENCE_FIELD = "SequenceNumber";
    public static final String COUNT_FIELD = "MessageCount";
    public static final String LENGTHS_FIELD = "MessageLengths";

    private final MoldUdp64CodecSettings settings;

    public MoldUdp64Codec(@NotNull MoldUdp64CodecSettings settings) {
        this.settings = Objects.requireNonNull(settings);
    }

    @NotNull
    @Override
    public MessageGroup encode(@NotNull MessageGroup messageGroup) {
        var messages = Objects.requireNonNull(messageGroup).getMessagesList();

        if (messages.isEmpty()) {
            return messageGroup;
        }

        Message header = null;
        AnyMessage firstMessage = messages.get(0);

        if (firstMessage.hasMessage()) {
            header = firstMessage.getMessage();
            var protocol = header.getMetadata().getProtocol();

            if (!protocol.isEmpty() && !protocol.equals(PROTOCOL)) {
                throw new IllegalArgumentException("Unexpected protocol: " + protocol + " (expected: " + PROTOCOL + ')');
            }

            String messageType = header.getMetadata().getMessageType();

            if (!HEADER_MESSAGE_TYPE.equals(messageType)) {
                throw new IllegalArgumentException("Unexpected header message type: " + messageType + " (expected: " + HEADER_MESSAGE_TYPE + ')');
            }
        }

        var payload = messages.stream()
                .skip(header == null ? 0 : 1)
                .peek(message -> {
                    if (message.hasMessage()) {
                        throw new IllegalArgumentException("All payload messages must be raw messages");
                    }
                })
                .map(AnyMessage::getMessage)
                .collect(Collectors.toUnmodifiableList());

        var builder = MessageGroup.newBuilder();

        // encode header and payload here

        return builder.build();
    }

    @NotNull
    @Override
    public MessageGroup decode(@NotNull MessageGroup messageGroup) {
        var messages = Objects.requireNonNull(messageGroup).getMessagesList();

        if (messages.isEmpty()) {
            return messageGroup;
        }

        if (messages.size() > 1) {
            throw new IllegalArgumentException("Message group contains more than 1 message");
        }

        var firstMessage = messages.get(0);

        if (!firstMessage.hasRawMessage()) {
            throw new IllegalArgumentException("Input message is not a raw message");
        }

        var message = firstMessage.getRawMessage();
        var protocol = message.getMetadata().getProtocol();

        if (!protocol.isEmpty() && !protocol.equals(PROTOCOL)) {
            throw new IllegalArgumentException("Unexpected protocol: " + protocol + " (expected: " + PROTOCOL + ')');
        }

        var builder = MessageGroup.newBuilder();

        // decode message's body here

        return builder.build();
    }

    @Override
    public void close() {
    }
}
