/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.kafka.connect.sink.cdc.hierarchical;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.cdc.CdcHandlerFactory;

public class HierarchicalHandlerFactory implements CdcHandlerFactory {

    // Конструктор по умолчанию
    public HierarchicalHandlerFactory() {
    }

    // Конструктор с параметром конфигурации
    public HierarchicalHandlerFactory(final MongoSinkTopicConfig config) {
        // Этот конструктор необходим для правильной инициализации через Kafka Connect
    }

    @Override
    public CdcHandler createHandler(final MongoSinkTopicConfig config) {
        return new HierarchicalRdbmsHandler(config);
    }
} 