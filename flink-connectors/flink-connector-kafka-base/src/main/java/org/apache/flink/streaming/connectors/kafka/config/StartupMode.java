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
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;

/**
 * Startup modes for the Kafka Consumer.
 */
@Internal
public enum StartupMode {

	/** Start from committed offsets in ZK / Kafka brokers of a specific consumer group (default). */
	//从 Kafka Topic 指定的消费组已提交的 offset 处开始消费
	GROUP_OFFSETS(KafkaTopicPartitionStateSentinel.GROUP_OFFSET),

	/** Start from the earliest offset possible. */
	//从最开始处开始消费
	EARLIEST(KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET),

	/** Start from the latest offset. */
	//从最新的 offset 开始消费
	LATEST(KafkaTopicPartitionStateSentinel.LATEST_OFFSET),

	/**
	 * Start from user-supplied timestamp for each partition.
	 * Since this mode will have specific offsets to start with, we do not need a sentinel value;
	 * using Long.MIN_VALUE as a placeholder.
	 */
	//每个分区从用户指定的时间戳开始消费
	TIMESTAMP(Long.MIN_VALUE),

	/**
	 * Start from user-supplied specific offsets for each partition.
	 * Since this mode will have specific offsets to start with, we do not need a sentinel value;
	 * using Long.MIN_VALUE as a placeholder.
	 */
	//每个分区从用户指定的 offset 开始消费
	SPECIFIC_OFFSETS(Long.MIN_VALUE);

	/** The sentinel offset value corresponding to this startup mode. */
	private long stateSentinel;

	StartupMode(long stateSentinel) {
		this.stateSentinel = stateSentinel;
	}

	public long getStateSentinel() {
		return stateSentinel;
	}
}
