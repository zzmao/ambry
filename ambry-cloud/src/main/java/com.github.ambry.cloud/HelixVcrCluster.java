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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.StaticClusterAgentsFactory;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.LeaderStandbySMD;


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
  private final HashSet<PartitionId> assignedPartitionIds = new HashSet<>();

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
    System.out.println("all part: " + allPartitions);
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
  }

  synchronized public void addPartition(String partitionIdStr) {
    System.out.println("addPar " + partitionIdStr);
    if (partitionIdMap.containsKey(partitionIdStr)) {
      assignedPartitionIds.add(partitionIdMap.get(partitionIdStr));
    }
  }

  synchronized public void removePartition(String partitionIdStr) {
    System.out.println("removePar " + partitionIdStr);
    if (partitionIdMap.containsKey(partitionIdStr)) {
      assignedPartitionIds.remove(partitionIdMap.get(partitionIdStr));
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
    return new LinkedList<>(assignedPartitionIds);
  }

  @Override
  public void close() {
    helixAdmin.close();
    manager.disconnect();
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Hello World!"); //Display the string.
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.resolve.hostnames", "false");
    props.setProperty("clustermap.cluster.name", "clusterName");
    props.setProperty("clustermap.datacenter.name", "Datacenter");
    props.setProperty("clustermap.ssl.enabled.datacenters", "Datacenter,DC1");
    props.setProperty("clustermap.port", "8123");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    props = new Properties();
    props.setProperty("vcr.ssl.port", "12345");
    props.setProperty(CloudConfig.VCR_CLUSTER_ZK_CONNECT_STRING, "zemao-ld1.linkedin.biz:2181");
    props.setProperty(CloudConfig.VCR_CLUSTER_NAME, "vcrCluster");
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(props));

    StaticClusterAgentsFactory staticClusterAgentsFactory =
        new StaticClusterAgentsFactory(clusterMapConfig, "/Users/zemao/ambry/config/HardwareLayoutMultiPartition.json",
            "/Users/zemao/ambry/config/PartitionLayoutMultiPartition.json");
    HelixVcrCluster h = new HelixVcrCluster(cloudConfig, clusterMapConfig, staticClusterAgentsFactory.getClusterMap());
    Thread.sleep(1000);
    System.out.println(h.getAssignedPartitionIds());
    System.out.println("Hello World done!"); //Display the string.
  }
}

