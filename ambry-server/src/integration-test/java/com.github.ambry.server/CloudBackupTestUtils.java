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
package com.github.ambry.server;

import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.commons.BlobId;
import com.github.ambry.utils.Pair;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


class MockCloudDestinationFactory implements CloudDestinationFactory {
  MockCloudDestination mockCloudDestination;

  MockCloudDestinationFactory(MockCloudDestination mockCloudDestination) {
    this.mockCloudDestination = mockCloudDestination;
  }

  @Override
  public CloudDestination getCloudDestination() throws IllegalStateException {
    return mockCloudDestination;
  }
}

class MockCloudDestination implements CloudDestination {

  private final Map<BlobId, Pair<CloudBlobMetadata, InputStream>> map = new HashMap<>();
  private final CountDownLatch latch;
  private final Set<BlobId> blobIds;

  MockCloudDestination(List<BlobId> blobIds) {
    this.blobIds = new HashSet<>(blobIds);
    this.latch = new CountDownLatch(blobIds.size());
  }

  @Override
  public boolean uploadBlob(BlobId blobId, long blobSize, CloudBlobMetadata cloudBlobMetadata,
      InputStream blobInputStream) throws CloudStorageException {
    if (blobIds.contains(blobId)) {
      map.put(blobId, new Pair<>(cloudBlobMetadata, blobInputStream));
      latch.countDown();
    }
    return true;
  }

  @Override
  public boolean deleteBlob(BlobId blobId, long deletionTime) throws CloudStorageException {
    return true;
  }

  @Override
  public boolean updateBlobExpiration(BlobId blobId, long expirationTime) throws CloudStorageException {
    return true;
  }

  @Override
  public Map<String, CloudBlobMetadata> getBlobMetadata(List<BlobId> blobIds) throws CloudStorageException {
    Map<String, CloudBlobMetadata> result = new HashMap<>();
    for (BlobId blobId : blobIds) {
      if (map.containsKey(blobId)) {
        result.put(blobId.toString(), map.get(blobId).getFirst());
      }
    }
    return result;
  }

  @Override
  public boolean doesBlobExist(BlobId blobId) throws CloudStorageException {
    return map.containsKey(blobId);
  }

  boolean await(long duration, TimeUnit timeUnit) throws InterruptedException {
    return latch.await(duration, timeUnit);
  }
}

class MockVirtualReplicatorCluster implements VirtualReplicatorCluster {

  DataNodeId dataNodeId;
  ClusterMap clusterMap;

  MockVirtualReplicatorCluster(DataNodeId dataNodeId, ClusterMap clusterMap) {
    this.dataNodeId = dataNodeId;
    this.clusterMap = clusterMap;
  }

  @Override
  public List<? extends DataNodeId> getAllDataNodeIds() {
    return null;
  }

  @Override
  public DataNodeId getCurrentDataNodeId() {
    return dataNodeId;
  }

  @Override
  public List<? extends PartitionId> getAssignedPartitionIds() {
    return clusterMap.getAllPartitionIds(null);
  }

  @Override
  public void close() throws Exception {

  }
}
