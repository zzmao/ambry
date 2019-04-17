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

import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.TestUtils;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test of HelixVcrClusterTest.
 */
public class HelixVcrClusterTest {
  private final static Logger logger = LoggerFactory.getLogger(HelixVcrClusterTest.class);
  private static MockClusterAgentsFactory mockClusterAgentsFactory;
  private static MockClusterMap mockClusterMap;
  private static final String ZK_SERVER_HOSTNAME = "localhost";
  private static final int ZK_SERVER_PORT = 31900;
  private static final String ZK_CONNECT_STRING = ZK_SERVER_HOSTNAME + ":" + Integer.toString(ZK_SERVER_PORT);
  private static TestUtils.ZkInfo zkInfo;
  private static final String VCR_CLUSTER_NAME = "vcrTestCluster";
  private static HelixControllerManager helixControllerManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    mockClusterAgentsFactory = new MockClusterAgentsFactory(false, 1, 1, 2);
    mockClusterMap = mockClusterAgentsFactory.getClusterMap();
    zkInfo = new TestUtils.ZkInfo(TestUtils.getTempDir("helixVcr"), "DC1", (byte) 1, ZK_SERVER_PORT, true);
    helixControllerManager =
        VcrTestUtil.populateZkInfoAndStartController(ZK_CONNECT_STRING, VCR_CLUSTER_NAME, mockClusterMap);
  }

  @AfterClass
  public static void afterClass() {
    helixControllerManager.syncStop();
    zkInfo.shutdown();
  }

  @Test
  public void helixVcrClusterFactoryTest() throws Exception {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.resolve.hostnames", "false");
    props.setProperty("clustermap.cluster.name", "clusterName");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.ssl.enabled.datacenters", "DC1,DC2");
    props.setProperty("clustermap.port", "8123");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));

    props = new Properties();
    props.setProperty("vcr.ssl.port", "12345");
    props.setProperty(CloudConfig.VCR_CLUSTER_ZK_CONNECT_STRING,
        ZK_SERVER_HOSTNAME + ":" + Integer.toString(ZK_SERVER_PORT));
    props.setProperty(CloudConfig.VCR_CLUSTER_NAME, VCR_CLUSTER_NAME);
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(props));

    VirtualReplicatorCluster helixVcrCluster =
        new HelixVcrClusterFactory(cloudConfig, clusterMapConfig, mockClusterMap).getVirtualReplicatorCluster();
    Thread.sleep(1000);
    Assert.assertEquals("Partition assignment not correct.", helixVcrCluster.getAssignedPartitionIds(),
        mockClusterMap.getAllPartitionIds(null));
    helixVcrCluster.close();
  }
}
