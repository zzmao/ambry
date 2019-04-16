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
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.LeaderStandbySMD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helix Based VCR Cluster.
 */
public class HelixVcrCluster implements VirtualReplicatorCluster {
  private final static Logger logger = LoggerFactory.getLogger(HelixVcrCluster.class);
  private final DataNodeId currentDataNode;
  private final String vcrClusterName;
  private final String vcrInstanceName;
  private final HelixManager manager;
  private final HelixAdmin helixAdmin;
  private final Map<String, PartitionId> partitionIdMap;
  private final HashSet<PartitionId> assignedPartitionIds = new HashSet<>();
  private final HelixVcrClusterMetrics metrics;

  /**
   * Construct the helix VCR cluster.
   * @param cloudConfig The cloud configuration to use.
   * @param clusterMapConfig The clustermap configuration to use.
   * @param clusterMap The clustermap to use.
   */
  public HelixVcrCluster(CloudConfig cloudConfig, ClusterMapConfig clusterMapConfig, ClusterMap clusterMap)
      throws Exception {
    currentDataNode = new CloudDataNode(cloudConfig, clusterMapConfig);
    List<? extends PartitionId> allPartitions = clusterMap.getAllPartitionIds(null);
    logger.trace("All partitions from clusterMap: " + allPartitions);
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
    metrics = new HelixVcrClusterMetrics(clusterMap.getMetricRegistry());
    logger.info("HelixVcrCluster started successfully.");
  }

  synchronized public void addPartition(String partitionIdStr) {
    if (partitionIdMap.containsKey(partitionIdStr)) {
      assignedPartitionIds.add(partitionIdMap.get(partitionIdStr));
      logger.info("Added partition {} to current VCR: ", partitionIdStr);
    } else {
      logger.trace("Partition {} not in clusterMap on add.", partitionIdStr);
      metrics.partitionIdNotInClusterMapOnAdd.inc();
    }
  }

  synchronized public void removePartition(String partitionIdStr) {
    if (partitionIdMap.containsKey(partitionIdStr)) {
      assignedPartitionIds.remove(partitionIdMap.get(partitionIdStr));
      logger.info("Removed partition {} to current VCR: ", partitionIdStr);
    } else {
      logger.trace("Partition {} not in clusterMap on remove.", partitionIdStr);
      metrics.partitionIdNotInClusterMapOnRemove.inc();
    }
  }

  @Override
  public List<? extends DataNodeId> getAllDataNodeIds() {
    // TODO: return all VCR nodes for recovery.
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
}
