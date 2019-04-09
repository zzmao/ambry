/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaEventType;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.LiveInstance;
import org.json.JSONObject;


/**
 * VCR Cluster based on static partition assignment.
 */
public class HelixVcrCluster implements VirtualReplicatorCluster {

  private final DataNodeId currentDataNode;
  private final String vcrClusterName;
  private final String vcrInstanceName;
  private final HelixManager manager;
  private final HelixAdmin helixAdmin;
  private final Map<String, PartitionId> partitionIdMap;
  private final List<PartitionId> assignedPartitionIds;

  /**
   * Construct the static VCR cluster.
   * @param cloudConfig The cloud configuration to use.
   * @param clusterMapConfig The clustermap configuration to use.
   * @param clusterMap The clustermap to use.
   */
  public HelixVcrCluster(CloudConfig cloudConfig, ClusterMapConfig clusterMapConfig, ClusterMap clusterMap)
      throws Exception {
    currentDataNode = new CloudDataNode(cloudConfig, clusterMapConfig);
    List<? extends PartitionId> allPartitions = clusterMap.getAllPartitionIds(null);
    partitionIdMap = allPartitions.stream().collect(Collectors.toMap(PartitionId::toPathString, Function.identity()));
    vcrClusterName = cloudConfig.vcrClusterName;
    vcrInstanceName =
        ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort);

    manager = HelixManagerFactory.getZKHelixManager(vcrClusterName, vcrInstanceName, InstanceType.PARTICIPANT,
        cloudConfig.vcrClusterZkConnectString);
    manager.getStateMachineEngine()
        .registerStateModelFactory(LeaderStandbySMD.name, new HelixVcrStateModelFactory(this));
    manager.connect();
    helixAdmin = manager.getClusterManagmentTool();
    IdealStateChangeListener listener = new IdealStateChangeListener() {
      @Override
      public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext)
          throws InterruptedException {
        for (IdealState is : idealState) {
          System.out.println(is.getPartitionSet());
        }
      }
    };
    manager.addIdealStateChangeListener(listener);
    manager.addLiveInstanceChangeListener(new LiveInstanceChangeListener() {
      @Override
      public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
        for (LiveInstance li : liveInstances) {
          li.getInstanceName();
        }
      }
    });

    assignedPartitionIds = new ArrayList<>();
    for (PartitionId id : assignedPartitionIds) {
      if (!partitionIdMap.containsKey(id)) {
        throw new IllegalArgumentException("Invalid partition specified: " + id);
      }
      assignedPartitionIds.add(partitionIdMap.get(id));
    }
  }

  @Override
  public List<? extends DataNodeId> getAllDataNodeIds() {
    return Collections.singletonList(currentDataNode);
  }

  @Override
  public DataNodeId getCurrentDataNodeId() {
    return currentDataNode;
  }

  @Override
  public List<? extends PartitionId> getAssignedPartitionIds() {
    return assignedPartitionIds;
  }

  @Override
  public void close() throws Exception {
    helixAdmin.close();
    manager.disconnect();
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Hello World!"); //Display the string.
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.resolve.hostnames", "false");
    props.setProperty("clustermap.cluster.name", "clusterName");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.ssl.enabled.datacenters", "DC0,DC1");
    props.setProperty("clustermap.port", "8900");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    props = new Properties();
    props.setProperty("vcr.ssl.port", "12345");
    props.setProperty(CloudConfig.VCR_CLUSTER_ZK_CONNECT_STRING, "zemao-ld1.linkedin.biz:2181");
    props.setProperty(CloudConfig.VCR_CLUSTER_NAME, "testCluster");
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(props));

    new HelixVcrCluster(cloudConfig, clusterMapConfig, new MockClusterMap());
    System.out.println("Hello World done!"); //Display the string.
    Thread.sleep(1000000);
  }
}

class MockClusterMap implements ClusterMap {

  @Override
  public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
    return null;
  }

  @Override
  public List<? extends PartitionId> getWritablePartitionIds(String partitionClass) {
    return null;
  }

  @Override
  public List<? extends PartitionId> getAllPartitionIds(String partitionClass) {
    return new ArrayList<>();
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return false;
  }

  @Override
  public byte getLocalDatacenterId() {
    return 0;
  }

  @Override
  public String getDatacenterName(byte id) {
    return null;
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    return null;
  }

  @Override
  public List<? extends ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    return null;
  }

  @Override
  public List<? extends DataNodeId> getDataNodeIds() {
    return null;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return null;
  }

  @Override
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {

  }

  @Override
  public JSONObject getSnapshot() {
    return null;
  }

  @Override
  public void close() {

  }
}
