/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.rest.Http2NetworkClient;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class ServerHttp2Test {
  private static Properties routerProps;
  private static MockNotificationSystem notificationSystem;
  private static MockCluster http2Cluster;
  private final boolean testEncryption;

  @BeforeClass
  public static void initializeTests() throws Exception {

    routerProps = new Properties();
    routerProps.setProperty("kms.default.container.key", TestUtils.getRandomKey(32));
    routerProps.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);

    Properties properties = new Properties();
    properties.setProperty("rest.server.rest.request.service.factory",
        "com.github.ambry.server.StorageRestRequestService");
    properties.setProperty("rest.server.nio.server.factory", "com.github.ambry.rest.StorageServerNettyFactory");
    properties.setProperty("ssl.client.authentication", "none");
    http2Cluster = new MockCluster(properties, false, SystemTime.getInstance(), 1, 1, 2);
    notificationSystem = new MockNotificationSystem(http2Cluster.getClusterMap());
    http2Cluster.initializeServers(notificationSystem);
    http2Cluster.startServers();
  }

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false},});
  }

  public ServerHttp2Test(boolean testEncryption) {
    this.testEncryption = testEncryption;
  }

  @AfterClass
  public static void cleanup() throws IOException {
    if (http2Cluster != null) {
      http2Cluster.cleanup();
    }
  }

  @Test
  public void clientTest() throws Exception {

    MockClusterMap clusterMap = http2Cluster.getClusterMap();
    DataNodeId dataNodeId = http2Cluster.getGeneralDataNode();

    byte[] usermetadata = new byte[1000];
    byte[] data = new byte[31870];
    byte[] encryptionKey = new byte[100];
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);

    BlobProperties properties = new BlobProperties(31870, "serviceid1", accountId, containerId, testEncryption);
    TestUtils.RANDOM.nextBytes(usermetadata);
    TestUtils.RANDOM.nextBytes(data);
    if (testEncryption) {
      TestUtils.RANDOM.nextBytes(encryptionKey);
    }
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    short blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    BlobId blobId1 = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
        properties.getAccountId(), properties.getContainerId(), partitionIds.get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    // put blob 1
    PutRequest putRequest =
        new PutRequest(1, "client1", blobId1, properties, ByteBuffer.wrap(usermetadata), ByteBuffer.wrap(data),
            properties.getBlobSize(), BlobType.DataBlob, testEncryption ? ByteBuffer.wrap(encryptionKey) : null);
    RequestInfo request =
        new RequestInfo(dataNodeId.getHostname(), new Port(dataNodeId.getHttp2Port(), PortType.HTTP2), putRequest,
            http2Cluster.getClusterMap().getReplicaIds(dataNodeId).get(0));

    Http2NetworkClient networkClient =
        new Http2NetworkClient(new SSLConfig(new VerifiableProperties(new Properties())));
    networkClient.sendAndPoll(Collections.singletonList(request), new HashSet<>(), 300);
    Thread.sleep(5000);
    List<ResponseInfo> responseInfos = networkClient.sendAndPoll(Collections.EMPTY_LIST, new HashSet<>(), 300);
    System.out.println(responseInfos);
    responseInfos = networkClient.sendAndPoll(Collections.EMPTY_LIST, new HashSet<>(), 300);
    System.out.println(responseInfos);
  }

  @Test
  public void endToEndTest() throws Exception {
    DataNodeId dataNodeId = http2Cluster.getGeneralDataNode();
    ServerTestUtil.endToEndTest(new Port(dataNodeId.getHttp2Port(), PortType.HTTP2), "DC1", http2Cluster, null, null,
        routerProps, testEncryption);
  }
}
