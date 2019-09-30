/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.io.network.partition.PartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import org.slf4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A scheduler that delegates to the scheduling logic in the {@link ExecutionGraph}.
 *
 * @see ExecutionGraph#scheduleForExecution()
 */
public class LegacyScheduler extends SchedulerBase {

	public LegacyScheduler(
			final Logger log,
			final JobGraph jobGraph,
			final BackPressureStatsTracker backPressureStatsTracker,
			final Executor ioExecutor,
			final Configuration jobMasterConfiguration,
			final SlotProvider slotProvider,
			final ScheduledExecutorService futureExecutor,
			final ClassLoader userCodeLoader,
			final CheckpointRecoveryFactory checkpointRecoveryFactory,
			final Time rpcTimeout,
			final RestartStrategyFactory restartStrategyFactory,
			final BlobWriter blobWriter,
			final JobManagerJobMetricGroup jobManagerJobMetricGroup,
			final Time slotRequestTimeout,
			final ShuffleMaster<?> shuffleMaster,
			final PartitionTracker partitionTracker) throws Exception {

		super(
			log,
			jobGraph,
			backPressureStatsTracker,
			ioExecutor,
			jobMasterConfiguration,
			slotProvider,
			futureExecutor,
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			restartStrategyFactory,
			blobWriter,
			jobManagerJobMetricGroup,
			slotRequestTimeout,
			shuffleMaster,
			partitionTracker);
	}

	@Override
	protected void startSchedulingInternal() {
		final ExecutionGraph executionGraph = getExecutionGraph();
		try {
			executionGraph.scheduleForExecution();
		}
		catch (Throwable t) {
			executionGraph.failGlobal(t);
		}
	}
<<<<<<< HEAD

	@Override
	public void suspend(Throwable cause) {
		mainThreadExecutor.assertRunningInMainThread();
		executionGraph.suspend(cause);
	}

	@Override
	public void cancel() {
		mainThreadExecutor.assertRunningInMainThread();
		executionGraph.cancel();
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return executionGraph.getTerminationFuture().thenApply(FunctionUtils.nullFn());
	}

	@Override
	public boolean updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
		mainThreadExecutor.assertRunningInMainThread();
		return executionGraph.updateState(taskExecutionState);
	}

	@Override
	public SerializedInputSplit requestNextInputSplit(JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
		mainThreadExecutor.assertRunningInMainThread();

		final Execution execution = executionGraph.getRegisteredExecutions().get(executionAttempt);
		if (execution == null) {
			// can happen when JobManager had already unregistered this execution upon on task failure,
			// but TaskManager get some delay to aware of that situation
			if (log.isDebugEnabled()) {
				log.debug("Can not find Execution for attempt {}.", executionAttempt);
			}
			// but we should TaskManager be aware of this
			throw new IllegalArgumentException("Can not find Execution for attempt " + executionAttempt);
		}

		final ExecutionJobVertex vertex = executionGraph.getJobVertex(vertexID);
		if (vertex == null) {
			throw new IllegalArgumentException("Cannot find execution vertex for vertex ID " + vertexID);
		}

		if (vertex.getSplitAssigner() == null) {
			throw new IllegalStateException("No InputSplitAssigner for vertex ID " + vertexID);
		}

		final InputSplit nextInputSplit = execution.getNextInputSplit();

		if (log.isDebugEnabled()) {
			log.debug("Send next input split {}.", nextInputSplit);
		}

		try {
			final byte[] serializedInputSplit = InstantiationUtil.serializeObject(nextInputSplit);
			return new SerializedInputSplit(serializedInputSplit);
		} catch (Exception ex) {
			IOException reason = new IOException("Could not serialize the next input split of class " +
				nextInputSplit.getClass() + ".", ex);
			vertex.fail(reason);
			throw reason;
		}
	}

	@Override
	public ExecutionState requestPartitionState(
			final IntermediateDataSetID intermediateResultId,
			final ResultPartitionID resultPartitionId) throws PartitionProducerDisposedException {

		mainThreadExecutor.assertRunningInMainThread();

		final Execution execution = executionGraph.getRegisteredExecutions().get(resultPartitionId.getProducerId());
		if (execution != null) {
			return execution.getState();
		}
		else {
			final IntermediateResult intermediateResult =
				executionGraph.getAllIntermediateResults().get(intermediateResultId);

			if (intermediateResult != null) {
				// Try to find the producing execution
				Execution producerExecution = intermediateResult
					.getPartitionById(resultPartitionId.getPartitionId())
					.getProducer()
					.getCurrentExecutionAttempt();

				if (producerExecution.getAttemptId().equals(resultPartitionId.getProducerId())) {
					return producerExecution.getState();
				} else {
					throw new PartitionProducerDisposedException(resultPartitionId);
				}
			} else {
				throw new IllegalArgumentException("Intermediate data set with ID "
					+ intermediateResultId + " not found.");
			}
		}
	}

	@Override
	public void scheduleOrUpdateConsumers(final ResultPartitionID partitionID) {
		mainThreadExecutor.assertRunningInMainThread();

		try {
			executionGraph.scheduleOrUpdateConsumers(partitionID);
		} catch (ExecutionGraphException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ArchivedExecutionGraph requestJob() {
		mainThreadExecutor.assertRunningInMainThread();
		return ArchivedExecutionGraph.createFrom(executionGraph);
	}

	@Override
	public JobStatus requestJobStatus() {
		return executionGraph.getState();
	}

	@Override
	public JobDetails requestJobDetails() {
		mainThreadExecutor.assertRunningInMainThread();
		return WebMonitorUtils.createDetailsForJob(executionGraph);
	}

	@Override
	public KvStateLocation requestKvStateLocation(final JobID jobId, final String registrationName) throws UnknownKvStateLocation, FlinkJobNotFoundException {
		mainThreadExecutor.assertRunningInMainThread();

		// sanity check for the correct JobID
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Lookup key-value state for job {} with registration " +
					"name {}.", jobGraph.getJobID(), registrationName);
			}

			final KvStateLocationRegistry registry = executionGraph.getKvStateLocationRegistry();
			final KvStateLocation location = registry.getKvStateLocation(registrationName);
			if (location != null) {
				return location;
			} else {
				throw new UnknownKvStateLocation(registrationName);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Request of key-value state location for unknown job {} received.", jobId);
			}
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void notifyKvStateRegistered(final JobID jobId, final JobVertexID jobVertexId, final KeyGroupRange keyGroupRange, final String registrationName, final KvStateID kvStateId, final InetSocketAddress kvStateServerAddress) throws FlinkJobNotFoundException {
		mainThreadExecutor.assertRunningInMainThread();

		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state registered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraph.getKvStateLocationRegistry().notifyKvStateRegistered(
					jobVertexId, keyGroupRange, registrationName, kvStateId, kvStateServerAddress);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void notifyKvStateUnregistered(final JobID jobId, final JobVertexID jobVertexId, final KeyGroupRange keyGroupRange, final String registrationName) throws FlinkJobNotFoundException {
		mainThreadExecutor.assertRunningInMainThread();

		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state unregistered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraph.getKvStateLocationRegistry().notifyKvStateUnregistered(
					jobVertexId, keyGroupRange, registrationName);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void updateAccumulators(final AccumulatorSnapshot accumulatorSnapshot) {
		mainThreadExecutor.assertRunningInMainThread();

		executionGraph.updateAccumulators(accumulatorSnapshot);
	}

	@Override
	public Optional<OperatorBackPressureStats> requestOperatorBackPressureStats(final JobVertexID jobVertexId) throws FlinkException {
		final ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexId);
		if (jobVertex == null) {
			throw new FlinkException("JobVertexID not found " +
				jobVertexId);
		}

		return backPressureStatsTracker.getOperatorBackPressureStats(jobVertex);
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(final String targetDirectory, final boolean cancelJob) {
		mainThreadExecutor.assertRunningInMainThread();

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		if (checkpointCoordinator == null) {
			throw new IllegalStateException(
				String.format("Job %s is not a streaming job.", jobGraph.getJobID()));
		} else if (targetDirectory == null && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
			log.info("Trying to cancel job {} with savepoint, but no savepoint directory configured.", jobGraph.getJobID());

			throw new IllegalStateException(
				"No savepoint directory configured. You can either specify a directory " +
					"while cancelling via -s :targetDirectory or configure a cluster-wide " +
					"default via key '" + CheckpointingOptions.SAVEPOINT_DIRECTORY.key() + "'.");
		}

		if (cancelJob) {
			checkpointCoordinator.stopCheckpointScheduler();
		}

		return checkpointCoordinator
			.triggerSavepoint(System.currentTimeMillis(), targetDirectory)
			.thenApply(CompletedCheckpoint::getExternalPointer)
			.handleAsync((path, throwable) -> {
				if (throwable != null) {
					if (cancelJob) {
						startCheckpointScheduler(checkpointCoordinator);
					}
					throw new CompletionException(throwable);
				} else if (cancelJob) {
					log.info("Savepoint stored in {}. Now cancelling {}.", path, jobGraph.getJobID());
					cancel();
				}
				return path;
			}, mainThreadExecutor);
	}

	private void startCheckpointScheduler(final CheckpointCoordinator checkpointCoordinator) {
		mainThreadExecutor.assertRunningInMainThread();

		if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
			try {
				checkpointCoordinator.startCheckpointScheduler();
			} catch (IllegalStateException ignored) {
				// Concurrent shut down of the coordinator
			}
		}
	}

	@Override
	public void acknowledgeCheckpoint(final JobID jobID, final ExecutionAttemptID executionAttemptID, final long checkpointId, final CheckpointMetrics checkpointMetrics, final TaskStateSnapshot checkpointState) {
		mainThreadExecutor.assertRunningInMainThread();

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		final AcknowledgeCheckpoint ackMessage = new AcknowledgeCheckpoint(
			jobID,
			executionAttemptID,
			checkpointId,
			checkpointMetrics,
			checkpointState);

		final String taskManagerLocationInfo = retrieveTaskManagerLocation(executionAttemptID);

		if (checkpointCoordinator != null) {
			ioExecutor.execute(() -> {
				try {
					checkpointCoordinator.receiveAcknowledgeMessage(ackMessage, taskManagerLocationInfo);
				} catch (Throwable t) {
					log.warn("Error while processing checkpoint acknowledgement message", t);
				}
			});
		} else {
			String errorMessage = "Received AcknowledgeCheckpoint message for job {} with no CheckpointCoordinator";
			if (executionGraph.getState() == JobStatus.RUNNING) {
				log.error(errorMessage, jobGraph.getJobID());
			} else {
				log.debug(errorMessage, jobGraph.getJobID());
			}
		}
	}

	@Override
	public void declineCheckpoint(final DeclineCheckpoint decline) {
		mainThreadExecutor.assertRunningInMainThread();

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		final String taskManagerLocationInfo = retrieveTaskManagerLocation(decline.getTaskExecutionId());

		if (checkpointCoordinator != null) {
			ioExecutor.execute(() -> {
				try {
					checkpointCoordinator.receiveDeclineMessage(decline, taskManagerLocationInfo);
				} catch (Exception e) {
					log.error("Error in CheckpointCoordinator while processing {}", decline, e);
				}
			});
		} else {
			String errorMessage = "Received DeclineCheckpoint message for job {} with no CheckpointCoordinator";
			if (executionGraph.getState() == JobStatus.RUNNING) {
				log.error(errorMessage, jobGraph.getJobID());
			} else {
				log.debug(errorMessage, jobGraph.getJobID());
			}
		}
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(final String targetDirectory, final boolean advanceToEndOfEventTime) {
		mainThreadExecutor.assertRunningInMainThread();

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator == null) {
			return FutureUtils.completedExceptionally(new IllegalStateException(
				String.format("Job %s is not a streaming job.", jobGraph.getJobID())));
		}

		if (targetDirectory == null && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
			log.info("Trying to cancel job {} with savepoint, but no savepoint directory configured.", jobGraph.getJobID());

			return FutureUtils.completedExceptionally(new IllegalStateException(
				"No savepoint directory configured. You can either specify a directory " +
					"while cancelling via -s :targetDirectory or configure a cluster-wide " +
					"default via key '" + CheckpointingOptions.SAVEPOINT_DIRECTORY.key() + "'."));
		}

		// we stop the checkpoint coordinator so that we are guaranteed
		// to have only the data of the synchronous savepoint committed.
		// in case of failure, and if the job restarts, the coordinator
		// will be restarted by the CheckpointCoordinatorDeActivator.
		checkpointCoordinator.stopCheckpointScheduler();

		final long now = System.currentTimeMillis();
		final CompletableFuture<String> savepointFuture = checkpointCoordinator
				.triggerSynchronousSavepoint(now, advanceToEndOfEventTime, targetDirectory)
				.thenApply(CompletedCheckpoint::getExternalPointer);

		final CompletableFuture<JobStatus> terminationFuture = executionGraph
			.getTerminationFuture()
			.handle((jobstatus, throwable) -> {

				if (throwable != null) {
					log.info("Failed during stopping job {} with a savepoint. Reason: {}", jobGraph.getJobID(), throwable.getMessage());
					throw new CompletionException(throwable);
				} else if (jobstatus != JobStatus.FINISHED) {
					log.info("Failed during stopping job {} with a savepoint. Reason: Reached state {} instead of FINISHED.", jobGraph.getJobID(), jobstatus);
					throw new CompletionException(new FlinkException("Reached state " + jobstatus + " instead of FINISHED."));
				}
				return jobstatus;
			});

		return savepointFuture.thenCompose((path) ->
			terminationFuture.thenApply((jobStatus -> path)));
	}

	private String retrieveTaskManagerLocation(ExecutionAttemptID executionAttemptID) {
		final Optional<Execution> currentExecution = Optional.ofNullable(executionGraph.getRegisteredExecutions().get(executionAttemptID));

		return currentExecution
			.map(Execution::getAssignedResourceLocation)
			.map(TaskManagerLocation::toString)
			.orElse("Unknown location");
	}
=======
>>>>>>> release-1.9
}
