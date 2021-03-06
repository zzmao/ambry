/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.replication.FindToken;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * {@link RemoteTokenTracker} tracks tokens from all peer replicas and updates them when handling metadata request from
 * peer node.
 */
public class RemoteTokenTracker {
  private static final String DELIMITER = ":";
  private final ReplicaId localReplica;
  // The key of peerReplicaAndToken is a string containing hostname and path of peer replica.
  // For example: localhost:/mnt/u001/p1
  private ConcurrentMap<String, FindToken> peerReplicaAndToken = new ConcurrentHashMap<>();

  public RemoteTokenTracker(ReplicaId localReplica) {
    this.localReplica = localReplica;
    localReplica.getPeerReplicaIds().forEach(r -> {
      String hostnameAndPath = r.getDataNodeId().getHostname() + DELIMITER + r.getReplicaPath();
      peerReplicaAndToken.put(hostnameAndPath, new StoreFindToken());
    });
  }

  /**
   * Update peer replica token within this tracker.
   * @param token the most recent token from peer replica.
   * @param remoteHostName the hostname of peer node (where the peer replica resides).
   * @param remoteReplicaPath the path of peer replica on remote peer node.
   */
  void updateTokenFromPeerReplica(FindToken token, String remoteHostName, String remoteReplicaPath) {
    // this already handles newly added peer replica (i.e. move replica)
    peerReplicaAndToken.put(remoteHostName + DELIMITER + remoteReplicaPath, token);
  }

  /**
   * Refresh the peerReplicaAndToken map in case peer replica has changed (i.e. new replica is added and old replica is removed)
   */
  void refreshPeerReplicaTokens() {
    ConcurrentMap<String, FindToken> newPeerReplicaAndToken = new ConcurrentHashMap<>();
    // this should remove peer replica that no longer exists (i.e original replica is moved to other node)
    localReplica.getPeerReplicaIds().forEach(r -> {
      String hostnameAndPath = r.getDataNodeId().getHostname() + DELIMITER + r.getReplicaPath();
      newPeerReplicaAndToken.put(hostnameAndPath,
          peerReplicaAndToken.getOrDefault(hostnameAndPath, new StoreFindToken()));
    });
    // atomic switch
    peerReplicaAndToken = newPeerReplicaAndToken;
  }

  /**
   * @return a snapshot of peer replica to token map.
   */
  Map<String, FindToken> getPeerReplicaAndToken() {
    return new HashMap<>(peerReplicaAndToken);
  }
}
