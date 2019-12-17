/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.config;

import org.apache.flink.annotation.Internal;

/**
 * The offset commit mode represents the behaviour of how offsets are externally committed
 * back to Kafka brokers / Zookeeper.
 *
 * 这个枚举类表示 Kafka 消息 offset 如何提交到 Kafka 或者 Zookeeper
 *
 * <p>The exact value of this is determined at runtime in the consumer subtasks.
 */
@Internal
public enum OffsetCommitMode {

	/** Completely disable offset committing. 禁止 offset 提交*/
	DISABLED,

	/** Commit offsets back to Kafka only when checkpoints are completed. 当 Checkpoint 完成时，才会把 offset 提交到 Kafka */
	ON_CHECKPOINTS,

	/** Commit offsets periodically back to Kafka, using the auto commit functionality of internal Kafka clients.
	 * 定期自动提交 offset 到 Kafka，定期的时间是按照配置的 auto.commit.interval.ms 的值，如果没有的话，默认是 60s 自动提交
	 * 还得看 enable.auto.commit 是否配置的 true，默认是 true
	 * */
	KAFKA_PERIODIC;
}
