// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.okera.recordservice.trino;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.okera.recordservice.core.FetchResult;
import com.okera.recordservice.core.Task;
import com.okera.recordservice.util.Preconditions;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RecordServiceSplit implements ConnectorSplit {
  private final String plannerHost;
  private final int plannerPort;
  private final String connectedUser;
  private final int workerRpcTimeoutMs;
  private final int localWorkerPort;
  private final RecordServiceTableHandle tableHandle;
  private final int totalParts;
  private final int partNumber;
  private final List<HostAddress> addresses;
  private final byte[] serializedTask;
  private final boolean compressionEnabled;
  private final String serviceName;
  private final String token;
  private final Set<FetchResult.RecordFormat> formats;
  private final long limit;
  private final int recordsQueueSize;
  private final boolean containsComplexTypes;
  private final int plannerTaskMaxRefreshSec;
  private final Long workerTaskMemLimit;
  private final Long workerDefaultPoolMemLimit;
  private final boolean forceWorkerAuth;
  private final boolean enablePlannerSSL;
  private final boolean enableWorkerSSL;

  // Deserialized task. Populated lazily.
  private Task task;

  @JsonCreator
  public RecordServiceSplit(
          @JsonProperty("plannerHost") String plannerHost,
          @JsonProperty("plannerPort") int plannerPort,
          @JsonProperty("connectedUser") String connectedUser,
          @JsonProperty("workerRpcTimeoutMs") int workerRpcTimeoutMs,
          @JsonProperty("localWorkerPort") int localWorkerPort,
          @JsonProperty("tableHandle") RecordServiceTableHandle tableHandle,
          @JsonProperty("partNumber") int partNumber,
          @JsonProperty("totalParts") int totalParts,
          @JsonProperty("addresses") List<HostAddress> addresses,
          @JsonProperty("serializedTask") byte[] serializedTask,
          @JsonProperty("serviceName") String serviceName,
          @JsonProperty("token") String token,
          @JsonProperty("compressionnEnabled") boolean compressionEnabled,
          @JsonProperty("formats") Set<FetchResult.RecordFormat> formats,
          @JsonProperty("limit") long limit,
          @JsonProperty("recordsQueueSize") int recordsQueueSize,
          @JsonProperty("containsComplexTypes") boolean containsComplexTypes,
          @JsonProperty("plannerTaskMaxRefreshSec") int plannerTaskMaxRefreshSec,
          @JsonProperty("workerTaskMemLimit") Long workerTaskMemLimit,
          @JsonProperty("workerDefaultPoolMemLimit") Long workerDefaultPoolMemLimit,
          @JsonProperty("forceWorkerAuth") boolean forceWorkerAuth,
          @JsonProperty("enablePlannerSSL") boolean enablePlannerSSL,
          @JsonProperty("enableWorkerSSL") boolean enableWorkerSSL) {
    Preconditions.checkState(partNumber >= 0, "partNumber must be >= 0");
    Preconditions.checkState(totalParts >= 1, "totalParts must be >= 1");
    Preconditions.checkState(totalParts > partNumber, "totalParts must be > partNumber");

    this.plannerHost = plannerHost;
    this.plannerPort = plannerPort;
    this.connectedUser = connectedUser;
    this.workerRpcTimeoutMs = workerRpcTimeoutMs;
    this.localWorkerPort = localWorkerPort;
    this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
    this.partNumber = partNumber;
    this.totalParts = totalParts;
    this.addresses = new ArrayList<HostAddress>(
        requireNonNull(addresses, "addresses is null"));
    this.serializedTask = requireNonNull(serializedTask);
    this.serviceName = serviceName;
    this.token = token;
    this.compressionEnabled = compressionEnabled;
    this.formats = formats;
    this.limit = limit;
    this.recordsQueueSize = recordsQueueSize;
    this.containsComplexTypes = containsComplexTypes;
    this.plannerTaskMaxRefreshSec = plannerTaskMaxRefreshSec;
    this.workerTaskMemLimit = workerTaskMemLimit;
    this.workerDefaultPoolMemLimit = workerDefaultPoolMemLimit;
    this.forceWorkerAuth = forceWorkerAuth;
    this.enablePlannerSSL = enablePlannerSSL;
    this.enableWorkerSSL = enableWorkerSSL;
  }

  @JsonProperty
  public RecordServiceTableHandle getTableHandle() {
    return tableHandle;
  }

  @JsonProperty
  public int getTotalParts() {
    return totalParts;
  }

  @JsonProperty
  public int getPartNumber() {
    return partNumber;
  }

  @Override
  @JsonProperty
  public List<HostAddress> getAddresses() {
    return addresses;
  }

  @JsonProperty
  public byte[] getSerializedTask() {
    return serializedTask;
  }

  @Override
  public Object getInfo() {
    return this;
  }

  @JsonProperty
  public String getServiceName() {
    return serviceName;
  }

  @JsonProperty
  public String getToken() {
    return token;
  }

  @JsonProperty
  public boolean isCompressionEnabled() {
    return compressionEnabled;
  }

  @JsonProperty
  public Set<FetchResult.RecordFormat> getFormats() {
    return formats;
  }

  @JsonProperty
  public long getLimit() {
    return limit;
  }

  @JsonProperty
  public int recordsQueueSize() {
    return recordsQueueSize;
  }

  @Override
  public boolean isRemotelyAccessible() {
    return true;
  }

  @JsonProperty
  public int getLocalWorkerPort() {
    return localWorkerPort;
  }

  @JsonProperty
  public String getPlannerHost() {
    return plannerHost;
  }

  @JsonProperty
  public int getPlannerPort() {
    return plannerPort;
  }

  @JsonProperty
  public String getConnectedUser() {
    return connectedUser;
  }

  @JsonProperty
  public int getWorkerRpcTimeoutMs()  { return workerRpcTimeoutMs; }

  @JsonProperty
  public boolean isContainsComplexTypes() { return containsComplexTypes; }

  @JsonProperty
  public int getPlannerTaskMaxRefreshSec()  { return plannerTaskMaxRefreshSec; }

  @JsonProperty
  public Long getWorkerTaskMemLimit() { return workerTaskMemLimit; }

  @JsonProperty
  public Long getWorkerDefaultPoolMemLimit()  { return workerDefaultPoolMemLimit; }

  @JsonProperty
  public boolean isForceWorkerAuth() { return forceWorkerAuth; }

  @JsonProperty
  public boolean isEnablePlannerSSL() { return enablePlannerSSL; }

  @JsonProperty
  public boolean isEnableWorkerSSL() { return enableWorkerSSL; }

  public Task getTask() {
    try {
      if (task == null) {
        task = Task.deserialize(serializedTask);
      }
    } catch (IOException e) {
      throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unable to deserialize task.", e);
    }
    return task;
  }
}
