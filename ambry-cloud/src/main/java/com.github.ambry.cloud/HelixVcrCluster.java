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
import com.github.ambry.clustermap.VirtualReplicatorClusterListener;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
  private static final Logger logger = LoggerFactory.getLogger(HelixVcrCluster.class);
  private final DataNodeId currentDataNode;
  private final String vcrClusterName;
  private final String vcrInstanceName;
  private final HelixManager manager;
  private final HelixAdmin helixAdmin;
  private final Map<String, PartitionId> partitionIdMap;
  private final Set<PartitionId> assignedPartitionIds = ConcurrentHashMap.newKeySet();
  private final HelixVcrClusterMetrics metrics;
  private final List<VirtualReplicatorClusterListener> listeners = new ArrayList<>();

  /**
   * Construct the helix VCR cluster.
   * @param cloudConfig The cloud configuration to use.
   * @param clusterMapConfig The clustermap configuration to use.
   * @param clusterMap The clustermap to use.
   */
  public HelixVcrCluster(CloudConfig cloudConfig, ClusterMapConfig clusterMapConfig, ClusterMap clusterMap)
      throws Exception {
    if (Utils.isNullOrEmpty(cloudConfig.VCR_CLUSTER_ZK_CONNECT_STRING)) {
      throw new IllegalArgumentException("Missing value for " + CloudConfig.VCR_CLUSTER_ZK_CONNECT_STRING);
    } else if (Utils.isNullOrEmpty(cloudConfig.VCR_CLUSTER_NAME)) {
      throw new IllegalArgumentException("Missing value for " + CloudConfig.VCR_CLUSTER_NAME);
    }

    currentDataNode = new CloudDataNode(cloudConfig, clusterMapConfig);
    List<? extends PartitionId> allPartitions = clusterMap.getAllPartitionIds(null);
    logger.trace("All partitions from clusterMap: {}.", allPartitions);
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

  /**
   * Add {@link PartitionId} to assignedPartitionIds set, if {@parm partitionIdStr} valid.
   * Used in {@link HelixVcrStateModel} if current VCR becomes leader of a partition.
   * @param partitionIdStr The partitionIdStr notified by Helix.
   */
  public void addPartition(String partitionIdStr) {
    PartitionId partitionId = partitionIdMap.get(partitionIdStr);
    if (partitionId != null) {
      assignedPartitionIds.add(partitionId);
      for (VirtualReplicatorClusterListener listener : listeners) {
        listener.onPartitionAdded(partitionId);
      }
      logger.info("Added partition {} to current VCR: ", partitionIdStr);
    } else {
      logger.trace("Partition {} not in clusterMap on add.", partitionIdStr);
      metrics.partitionIdNotInClusterMapOnAdd.inc();
    }
  }

  /**
   * Remove {@link PartitionId} from assignedPartitionIds set, if {@parm partitionIdStr} valid.
   * Used in {@link HelixVcrStateModel} if current VCR becomes offline or standby a partition.
   * @param partitionIdStr The partitionIdStr notified by Helix.
   */
  public void removePartition(String partitionIdStr) {
    PartitionId partitionId = partitionIdMap.get(partitionIdStr);
    if (partitionId != null) {
      assignedPartitionIds.remove(partitionIdMap.get(partitionIdStr));
      for (VirtualReplicatorClusterListener listener : listeners) {
        listener.onPartitionRemoved(partitionId);
      }
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
  public void addListener(VirtualReplicatorClusterListener listener) {
    listeners.add(listener);
  }

  @Override
  public void close() {
    helixAdmin.close();
    manager.disconnect();
  }
}

