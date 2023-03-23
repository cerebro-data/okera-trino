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

import com.okera.recordservice.core.ConnectionCache;
import com.okera.recordservice.core.ConnectionContext;
import com.okera.recordservice.core.RecordServiceException;
import com.okera.recordservice.core.RecordServicePlannerClient;
import com.okera.recordservice.core.Table;
import com.okera.recordservice.security.AuthUtil;
import com.okera.recordservice.security.TokenUtil;
import com.okera.recordservice.util.ClusterUtil;
import com.okera.recordservice.util.ParseUtil;
import com.okera.recordservice.util.UserToken;
import io.airlift.configuration.Config;
import io.airlift.log.Logger;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import java.io.IOException;
import java.util.Random;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.StringUtils;

/**
 * This class manages the configs for the both the okera and recordservice connectors.
 * The recordservice configs are there for backwards compat and should not be modified.
 */
public class RecordServiceConfig {
  public static final String TRINO_APP =
      System.getenv("OKERA_ANALYTICS_CLUSTER") == null ? "trino" : "okera-trino";
  static final String PLANNER_HOST =
      System.getenv("ODAS_TEST_HOST") != null ?
      System.getenv("ODAS_TEST_HOST") : "localhost";
  static final int PLANNER_PORT =
      System.getenv("ODAS_TEST_PORT_PLANNER_THRIFT") != null ?
      Integer.parseInt(System.getenv("ODAS_TEST_PORT_PLANNER_THRIFT")) : 12050;

  // If determining max tasks automatically, the min and max to control the computed
  // value. This is picked to be in the known range of what we manually configure to
  // control the range of behavior. As we get more experience in auto-turning, we can
  // either remove or relax these.
  private static final int AUTO_MAX_TASKS_MIN_TASKS = 64;
  private static final int AUTO_MAX_TASKS_MAX_TASKS = 5128;

  private static final Logger LOG = Logger.get(RecordServiceConfig.class);

  // expected format of hostports is a list of hosts and ports separated with commas
  private String host = PLANNER_HOST;
  private int port = PLANNER_PORT;
  private String hostports = host + ":" + port;
  private int plannerRpcTimeoutMs = -1;
  private int workerRpcTimeoutMs = -1;
  private int maxTasks = -1;
  private boolean autoMaxTasksEnabled = true;
  private boolean compressionEnabled = false;
  private int localWorkerPort = -1;
  private String serviceName = "cerebro";
  private String token = null;
  private String lastNBytes = null;
  private int recordsQueueSize = 3;
  private Boolean enableExternalViews = null;
  private int metadataCacheTtlMs = 0;
  private boolean allowExternalRestrictedTasks = false;
  private int plannerTaskMaxRefreshSec = -1;
  private Table.StatsMode statsMode = Table.StatsMode.HMS;
  private Long workerTaskMemLimit = -1L;
  private Long workerDefaultPoolMemLimit = -1L;
  private boolean forceWorkerAuth = false;
  private boolean enablePlannerSSL = false;
  private boolean enableWorkerSSL = false;
  private String extraCredentialsTokenKey = 
      RecordServiceSessionProperties.ACCESS_TOKEN;


  @NotNull
  public String getHost() { return host; }
  public int getPort() { return port; }
  @NotNull
  public String getHostPorts() { return hostports; }
  public int getPlannerRpcTimeoutMs() { return plannerRpcTimeoutMs; }
  public int getWorkerRpcTimeoutMs()  { return workerRpcTimeoutMs;  }
  public boolean isCompressionEnabled() { return compressionEnabled; }
  public int getLocalWorkerPort() { return localWorkerPort; }
  public int getMaxTasks() { return maxTasks; }
  public boolean isAutoMaxTasksEnabled() { return autoMaxTasksEnabled; }
  public String getLastNBytes() { return lastNBytes; }
  public int getRecordsQueueSize() { return recordsQueueSize; }
  public Boolean isEnableExternalViews() { return enableExternalViews; }
  public int getMetadataCacheTtlMs() { return metadataCacheTtlMs; }
  public boolean isAllowExternalRestrictedTasks() {
    return allowExternalRestrictedTasks;
  }
  public int getPlannerTaskMaxRefreshSec() { return plannerTaskMaxRefreshSec; }
  public Table.StatsMode getStatsMode() {
    return statsMode;
  }

  public Table.StatsMode getStatsMode(ConnectorSession session) {
    Table.StatsMode returnStatsMode = this.statsMode;
    try {
      String sessionStatsMode = session.getProperty(
          RecordServiceSessionProperties.STATS_MODE,
          String.class
      );
      if (sessionStatsMode != null && !sessionStatsMode.isEmpty()) {
        returnStatsMode = Table.StatsMode.valueOf(sessionStatsMode);
        LOG.debug("Setting stats mode to " + sessionStatsMode + " from session property");
      }
    } catch (TrinoException e) {
      LOG.debug("Error parsing stats mode from session");
    }
    return returnStatsMode;
  }

  public Long getWorkerTaskMemLimit() { return workerTaskMemLimit; }
  public Long getWorkerDefaultPoolMemLimit()  { return workerDefaultPoolMemLimit; }
  public boolean isForceWorkerAuth() { return forceWorkerAuth; }
  public boolean isPlannerSSL() { return enablePlannerSSL; }
  public boolean isWorkerSSL() { return enableWorkerSSL; }
  public String getExtraCredentialsTokenKey() { return extraCredentialsTokenKey; }

  @Config("okera.planner.hostports")
  public RecordServiceConfig setHostPorts(String hostports) {
    this.hostports = hostports;
    parseHostPorts();
    LOG.info("Setting planner host:port to: " + host + ":" + port);
    return this;
  }

  @Config("okera.planner.rpc.timeout-ms")
  public RecordServiceConfig setPlannerRpcTimeoutMs(int timeout) {
    this.plannerRpcTimeoutMs = timeout;
    LOG.info("Setting planner timeout to: " + timeout);
    return this;
  }

  @Config("okera.worker.rpc.timeout-ms")
  public RecordServiceConfig setWorkerRpcTimeoutMs(int timeout) {
    this.workerRpcTimeoutMs = timeout;
    LOG.info("Setting worker timeout to: " + timeout);
    return this;
  }

  @Config("okera.task.plan.max-tasks")
  public RecordServiceConfig setMaxTasks(int tasks) {
    this.maxTasks = tasks;
    LOG.info("Setting maxTasks to: " + tasks);
    return this;
  }

  @Config("okera.task.plan.enable-auto-max-tasks")
  public RecordServiceConfig setAutoMaxTasksEnabled(boolean enabled) {
    this.autoMaxTasksEnabled = enabled;
    LOG.info("Setting autoMaxTasks enabled to: " + enabled);
    return this;
  }

  @Config("okera.worker.records-queue-size")
  public RecordServiceConfig setRecordsQueueSize(int v) {
    this.recordsQueueSize = v;
    LOG.info("Setting records queue size to: " + v);
    return this;
  }

  @Config("okera.task.plan.enable-compression")
  public RecordServiceConfig setCompressionEnabled(boolean enabled) {
    this.compressionEnabled = enabled;
    LOG.info("Setting compression enabled to: " + enabled);
    return this;
  }

  @Config("okera.sample.last-n-bytes")
  public RecordServiceConfig setLastNBytes(String v) {
    this.lastNBytes = v;
    LOG.info("Setting last N bytes to: " + v);
    return this;
  }

  @Config("okera.workers.local-port")
  public RecordServiceConfig setLocalWorkerPort(int v) {
    this.localWorkerPort = v;
    LOG.info("Setting local worker port to: " + v);
    return this;
  }

  @Config("okera.trino.enable-external-views")
  public RecordServiceConfig setEnableExternalViews(Boolean v) {
    this.enableExternalViews = v;
    LOG.info("Setting enable external views to: " + v);
    return this;
  }

  @Config("okera.metadata.cache-ttl-ms")
  public RecordServiceConfig setMetadataCacheTtlMs(int v) {
    this.metadataCacheTtlMs = v;
    LOG.info("Setting metadata cache TTL to: " + v + "ms");
    return this;
  }

  @Config("okera.task.plan.allow-external-restricted-tasks")
  public RecordServiceConfig setAllowExternalRestrictedTasks(boolean v) {
    this.allowExternalRestrictedTasks = v;
    LOG.info("Setting allowExternalRestrictedTasks to: " + v);
    return this;
  }

  @Config("okera.task.plan.max-refresh-sec")
  public RecordServiceConfig setPlannerTaskMaxRefreshSec(int v) {
    this.plannerTaskMaxRefreshSec = v;
    LOG.info("Setting task refresh max interval to: " + v + "sec");
    return this;
  }

  @Config("okera.task.plan.enable-okera-stats")
  public RecordServiceConfig setStatsMode(String v) {
    this.statsMode = Table.StatsMode.valueOf(v);
    LOG.info("Setting stats mode to: " + v);
    return this;
  }

  @Config("okera.task.memlimit.bytes")
  public RecordServiceConfig setWorkerTaskMemLimit(Long v)  {
    this.workerTaskMemLimit = v;
    LOG.info("Setting worker per-task mem limit in bytes to: " + v);
    return this;
  }

  @Config("okera.worker.default-pool-memory.bytes")
  public RecordServiceConfig setWorkerDefaultPoolMemLimit(Long v) {
    this.workerDefaultPoolMemLimit = v;
    LOG.info("Setting worker default-pool mem limit in byts to: " + v);
    return this;
  }

  @Config("okera.worker.force-auth")
  public RecordServiceConfig setForceWorkerAuth(Boolean v) {
    this.forceWorkerAuth = v;
    LOG.info("Setting force worker auth to: " + v);
    return this;
  }

  @Config("okera.planner.connection.ssl")
  public RecordServiceConfig setPlannerSSL(Boolean v) {
    this.enablePlannerSSL = v;
    LOG.info("Setting planner SSL to: " + v);
    return this;
  }

  @Config("okera.worker.connection.ssl")
  public RecordServiceConfig setWorkerSSL(Boolean v) {
    this.enableWorkerSSL = v;
    LOG.info("Setting worker SSL to: " + v);
    return this;
  }

  @Config("okera.extra-credentials.token.key")
  public RecordServiceConfig setExtraCredentialsTokenKey(String v) {
    this.extraCredentialsTokenKey = v;
    LOG.info("Setting extra-credentials token key to: " + v);
    return this;
  }

  @NotNull
  public String getServiceName() { return serviceName; }
  public String getToken() { return token; }

  /************************************************************************************
   * End configs.
   ************************************************************************************/

  /**
   * Returns the token string to use to authenticate requests. If this is set in the
   * session properties, that will be returned. If not and is globally configured for
   * the connector, the global value will be returned. Otherwise, if the user appears
   * to be a token, return the user. We do this because the session token does not
   * seem to work when retrieving metadata (e.g. list databases) but only for scans. This
   * makes the CLI very hard to use.
   *
   * If none of the above are set, return null.
   */
  public String getToken(ConnectorSession session) throws IOException {
    // Facebook presto jdbc driver throws an exception on session.getProperty()
    // with message like "unknown connector recordservice". Code inspection indicates
    // the list of session properties maintained in the presto layer ir empty. This
    // may be a bug.
    // In this case, fall through to the code path to determine the correct token.
    LOG.info("Session: " + session);
    LOG.info("Session Identity: " + session.getIdentity());
    String sessionToken = null;
    try {
      sessionToken = session.getProperty(
          RecordServiceSessionProperties.ACCESS_TOKEN, String.class);
    } catch (TrinoException e) {
      LOG.debug("Failed to get token from session properties.");
    }
    // try extra credentials "token" as well
    if (sessionToken == null && extraCredentialsTokenKey != null) {
      try {
        sessionToken = session.getIdentity().getExtraCredentials().get(
            extraCredentialsTokenKey);
      } catch (Exception e) {
        LOG.debug("Failed to get token from extra credentials.");
      }  
    }
    if (StringUtils.isNotEmpty(sessionToken)) {
      LOG.info("Session token found: " + TokenUtil.printableTokenString(sessionToken));
      return sessionToken;
    }
    if (StringUtils.isNotEmpty(this.token)) {
      LOG.info("Token found: " + TokenUtil.printableTokenString(this.token));
      return this.token;
    }

    // User looks like a token, just return that
    if (session.getUser().contains(".")) {
      LOG.info("Token found in session: " +
          TokenUtil.printableTokenString(session.getUser()));
      return session.getUser();
    }
    return getToken(session.getUser());
  }

  public String getToken(String userName) throws IOException  {
    String token = UserToken.tryGetUserToken(userName);
    // We log the failure to find a token here, in our last test, so that all
    // execution paths log at least once.
    if (ParseUtil.nullOrEmpty(token)) {
      LOG.info("No token found");
    } else {
      LOG.info("Using token: " + TokenUtil.printableTokenString(token));
    }
    return token;
  }

  // The form of a HostPort is <host>:<port number>. This function iterates over a list
  // of HostPorts and validates that each matches the expected format. It also sets the
  // host and port to connect to, to a random hostport from the list.
  private void parseHostPorts() {
    String[] individualHostPorts = hostports.split(",");
    String[] hostPort;

    Random rand = new Random();
    int randomValue = rand.nextInt(individualHostPorts.length);
    for (int i = 0; i < individualHostPorts.length; i++) {
      // Verify that the hostport is both colon-delimited, and consists of two components.
      hostPort = individualHostPorts[i].split(":");
      if (hostPort.length != 2) {
        throw new IllegalArgumentException(
            "Invalid hostport: " + individualHostPorts[i] +
            ". Expecting <hostname>:<port>");
      }
      // Verify that the port component of a hostport is an integer.
      try {
        Integer.parseInt(hostPort[1]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid hostport: " + individualHostPorts[i] +
            ". Expecting <hostname>:<port>");
      }
      // If this was the randomly chosen hostport, then set these values as the ones that
      // the Trino client should connect to.
      if (i == randomValue) {
        host = hostPort[0];
        port = Integer.parseInt(hostPort[1]);
      }
    }
  }

  // This function returns a RecordServicePlannerClient instance.
  // If impersonation is disabled, the ConnectionContext uses the token of the session
  // user.
  // If impersonation is enabled, the ConnectionContext uses the token of the presto user
  // to connect, which in turn impersonates the impersonatedUser.
  // NOTE: Do not forget to call close on the returned object.
  public RecordServicePlannerClient getPlanner(ConnectorSession session)
      throws IOException, RecordServiceException {
    return getPlanner(session, null);
  }

  public RecordServicePlannerClient getPlanner(ConnectorSession session,
      NodeManager nodeManager) throws IOException, RecordServiceException {
    ConnectionContext ctx = new ConnectionContext(TRINO_APP)
        .setHostPort(getHost(), getPort());
    RecordServiceUtil.populateRequestContext(ctx, session);

    String sessionUser = session.getUser();
    String authenticatedUser = null;
    if (session.getIdentity().getPrincipal().isPresent()) {
      authenticatedUser = session.getIdentity().getPrincipal().get().getName();
    }

    // Presto allows to specify a different session user than the authenticated
    // user, so we ensure that this delegation works.
    if (!AuthUtil.canDelegate(authenticatedUser, sessionUser)) {
      String errorMsg = String.format(
          "User '%s' is not allowed to delegate for user '%s'",
          authenticatedUser, sessionUser);
      throw new IOException(errorMsg);
    }

    String connectedUser = RecordServiceUtil.getConnectedUserName(session);
    if (System.getenv("OKERA_ALLOW_IMPERSONATION") == null) {
      String token = getToken(session);
      ctx.setToken(getServiceName(), token, connectedUser);
    } else {
      // This will set the session user as the user to impersonate.
      ctx.setImpersonatedUser(session.getUser());
      // If impersonating, connect as the presto user, whose home dir contains a token for
      // a user that is allowed to impersonate other users.
      String token = getToken(connectedUser);
      ctx.setToken(getServiceName(), token, connectedUser);
    }

    long sampling;
    try {
      sampling = session.getProperty(
          RecordServiceSessionProperties.SAMPLING_VALUE,
          Long.class
      );
      LOG.info("Setting sampling to " + sampling + " from session property");
    } catch (TrinoException e) {
      sampling = -1;
    }

    if (sampling > 0) {
      ctx.setSampleMaxDataSizeBytes(sampling);
    } else if (System.getenv("OKERA_SAMPLE_MAX_DATA_SIZE") != null) {
      final String value = System.getenv("OKERA_SAMPLE_MAX_DATA_SIZE");
      ctx.setSampleMaxDataSizeBytes(ParseUtil.parseMemoryString(value));
    } else if (lastNBytes != null) {
      ctx.setSampleMaxDataSizeBytes(ParseUtil.parseMemoryString(lastNBytes));
    }

    if (getPlannerRpcTimeoutMs() > 0) {
      ctx.setRpcTimeoutMs(getPlannerRpcTimeoutMs());
    }
    if (isCompressionEnabled()) {
      ctx.setEnableCompression(true);
    }
    int maxTasks = getMaxTasks();
    try {
      Integer sessionMaxTasks = session.getProperty(
          RecordServiceSessionProperties.MAX_TASKS,
          Integer.class
      );
      if (sessionMaxTasks != null && sessionMaxTasks > 0) {
        maxTasks = sessionMaxTasks;
        LOG.debug("Setting max tasks to " + sessionMaxTasks + " from session property");
      }
    } catch (TrinoException e) {
      LOG.debug("Error parsing max tasks from session");
    }
    if (maxTasks > 0)  {
      ctx.setMaxTasks(maxTasks);
    } else if (isAutoMaxTasksEnabled() && nodeManager != null) {
      int clusterSize = nodeManager.getWorkerNodes().size();
      int numCores = ClusterUtil.getNumCores();
      LOG.debug("Computed cluster size: %d, num cores: %s", clusterSize, numCores);
      if (clusterSize > 0 && numCores > 0) {
        final int taskMultiple = 8;
        maxTasks = clusterSize * numCores * taskMultiple;
        maxTasks = Math.min(maxTasks, AUTO_MAX_TASKS_MAX_TASKS);
        maxTasks = Math.max(maxTasks, AUTO_MAX_TASKS_MIN_TASKS);
        ctx.setMaxTasks(maxTasks);
      }
    }
    if (isPlannerSSL()) {
      ctx.setSSL(isPlannerSSL());
    }
    return ConnectionCache.defaultCache().getPlanner(connectedUser, null, ctx);
  }
}
