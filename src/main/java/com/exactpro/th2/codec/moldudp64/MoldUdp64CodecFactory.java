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

import java.io.InputStream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.codec.api.IPipelineCodec;
import com.exactpro.th2.codec.api.IPipelineCodecContext;
import com.exactpro.th2.codec.api.IPipelineCodecFactory;
import com.exactpro.th2.codec.api.IPipelineCodecSettings;

public class MoldUdp64CodecFactory implements IPipelineCodecFactory {
    public static final String PROTOCOL = "moldudp64";

    @NotNull
    @Override
    public Class<? extends IPipelineCodecSettings> getSettingsClass() {
        return MoldUdp64CodecSettings.class;
    }

    @NotNull
    @Override
    public String getProtocol() {
        return PROTOCOL;
    }

    @Override
    public void init(@NotNull InputStream stream) {
    }

    @Override
    public void init(@NotNull IPipelineCodecContext context) {
    }

    @NotNull
    @Override
    public IPipelineCodec create(@Nullable IPipelineCodecSettings settings) {
        if (settings instanceof MoldUdp64CodecSettings) {
            return new MoldUdp64Codec((MoldUdp64CodecSettings)settings);
        }

        throw new IllegalArgumentException("settings is not an instance of " + MoldUdp64CodecSettings.class.getSimpleName() + ": " + settings);
    }

    @Override
    public void close() {
    }
}
