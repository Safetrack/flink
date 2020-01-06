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

package org.apache.flink.batch.connectors.cassandra;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * OutputFormat to write data to Apache Cassandra and from a custom Cassandra annotated object.
 *
 * @param <OUT> type of outputClass
 */
public class CassandraPojoOutputFormat<OUT> extends RichOutputFormat<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraPojoOutputFormat.class);
	private static final long serialVersionUID = -1701885135103942460L;

	private final ClusterBuilder builder;

	private final MapperOptions mapperOptions;
	private final Class<OUT> outputClass;

	private transient Cluster cluster;
	private transient Session session;
	private transient Mapper<OUT> mapper;
	private transient FutureCallback<Void> callback;
	private transient Throwable exception = null;
	private Semaphore semaphore;

	public CassandraPojoOutputFormat(ClusterBuilder builder, Class<OUT> outputClass) {
		this(builder, outputClass, null);
	}

	public CassandraPojoOutputFormat(ClusterBuilder builder, Class<OUT> outputClass, MapperOptions mapperOptions) {
		Preconditions.checkNotNull(outputClass, "OutputClass cannot be null");
		Preconditions.checkNotNull(builder, "Builder cannot be null");
		this.builder = builder;
		this.mapperOptions = mapperOptions;
		this.outputClass = outputClass;
	}

	@Override
	public void configure(Configuration parameters) {
		this.cluster = builder.getCluster();
	}

	/**
	 * Opens a Session to Cassandra and initializes the prepared statement.
	 *
	 * @param taskNumber The number of the parallel instance.
	 */
	@Override
	public void open(int taskNumber, int numTasks) {
		this.session = cluster.connect();
		MappingManager mappingManager = new MappingManager(session);
		this.mapper = mappingManager.mapper(outputClass);
		if (mapperOptions != null) {
			Mapper.Option[] optionsArray = mapperOptions.getMapperOptions();
			if (optionsArray != null) {
				mapper.setDefaultSaveOptions(optionsArray);
			}
		}
		this.callback = new FutureCallback<Void>() {
			@Override
			public void onSuccess(Void ignored) {
				semaphore.release();
				onWriteSuccess();
			}

			@Override
			public void onFailure(Throwable t) {
				semaphore.release();
				onWriteFailure(t);
			}
		};
		semaphore = new Semaphore(Integer.MAX_VALUE);
	}

	@Override
	public void writeRecord(OUT record) throws IOException {
		if (exception != null) {
			throw new IOException("write record failed", exception);
		}
		if (!semaphore.tryAcquire()) {
			throw new IllegalStateException(String.format("Failed to acquire lock %d of %d is taken", Integer.MAX_VALUE, Integer.MAX_VALUE));
		}
		ListenableFuture<Void> result = mapper.saveAsync(record);
		Futures.addCallback(result, callback);
	}


	/**
	 * Callback that is invoked after a record is written to Cassandra successfully.
	 *
	 * <p>Subclass can override to provide its own logic.
	 */
	protected void onWriteSuccess() {
	}

	/**
	 * Callback that is invoked when failing to write to Cassandra.
	 * Current implementation will record the exception and fail the job upon next record.
	 *
	 * <p>Subclass can override to provide its own failure handling logic.
	 * @param t the exception
	 */
	protected void onWriteFailure(Throwable t) {
		exception = t;
	}

	/**
	 * Closes all resources used.
	 */
	@Override
	public void close() {
		mapper = null;
		try {
			semaphore.tryAcquire(Integer.MAX_VALUE, Integer.MAX_VALUE, TimeUnit.SECONDS); //Waiting until finished or interrupted
		} catch (InterruptedException e) {
			LOG.error("Interrupted while waiting for in-flight queries", e);
		}
		try {
			if (session != null) {
				session.close();
			}
		} catch (Exception e) {
			LOG.error("Error while closing session.", e);
		}

		try {
			if (cluster != null) {
				cluster.close();
			}
		} catch (Exception e) {
			LOG.error("Error while closing cluster.", e);
		}
	}
}
