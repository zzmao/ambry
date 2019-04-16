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

import com.github.ambry.clustermap.HelixAdminFactory;
import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.TestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.SharedZkClientFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;
import org.junit.Before;
import org.junit.Test;


/**
 * Test of HelixVcrClusterTest.
 */
public class HelixVcrClusterTest {
  private MockClusterAgentsFactory mockClusterAgentsFactory;
  private MockClusterMap mockClusterMap;
  private String ZK_SERVER_HOSTNAME = "localhost";
  private int ZK_SERVER_PORT = 31900;
  private String ZK_CONNECT_STRING = ZK_SERVER_HOSTNAME + ":" + Integer.toString(ZK_SERVER_PORT);
  private String DC_NAME = "DC1";
  private byte DC_ID = (byte) 1;
  private TestUtils.ZkInfo zkInfo;
  private String VCR_CLUSTER_NAME = "vcrTestCluster";

  @Before
  public void setup() throws Exception {
    mockClusterAgentsFactory = new MockClusterAgentsFactory(false, 1, 1, 2);
    mockClusterMap = mockClusterAgentsFactory.getClusterMap();
    zkInfo = new TestUtils.ZkInfo(TestUtils.getTempDir("helixVcr"), DC_NAME, DC_ID, ZK_SERVER_PORT, true);

    System.out.println("zk start done");
    String zkConnectString = ZK_SERVER_HOSTNAME + ":" + Integer.toString(ZK_SERVER_PORT);
    HelixZkClient zkClient =
        SharedZkClientFactory.getInstance().buildZkClient(new HelixZkClient.ZkConnectionConfig(zkConnectString));
    zkClient.setZkSerializer(new ZNRecordSerializer());
    ClusterSetup clusterSetup = new ClusterSetup(zkClient);
    clusterSetup.addCluster(VCR_CLUSTER_NAME, true);
    HelixAdmin admin = new HelixAdminFactory().getHelixAdmin(zkConnectString);
    // set ALLOW_PARTICIPANT_AUTO_JOIN
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
        forCluster(VCR_CLUSTER_NAME).build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    // setPersistBestPossibleAssignment
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(VCR_CLUSTER_NAME);
    clusterConfig.setPersistBestPossibleAssignment(true);
    configAccessor.setClusterConfig(VCR_CLUSTER_NAME, clusterConfig);

    String resourceName = "1";
    FullAutoModeISBuilder builder = new FullAutoModeISBuilder(resourceName);
    builder.setStateModel(LeaderStandbySMD.name);
    for (int i = 0; i < 12; i++) {
      builder.add(Integer.toString(i));
    }
    builder.setRebalanceStrategy(CrushRebalanceStrategy.class.getName());
    IdealState idealState = builder.build();
    admin.addResource(VCR_CLUSTER_NAME, resourceName, idealState);
    admin.rebalance(VCR_CLUSTER_NAME, resourceName, 3, "", "");
    System.out.println("zk setup Done");

    HelixControllerManager helixControllerManager = new HelixControllerManager(ZK_CONNECT_STRING, VCR_CLUSTER_NAME);
    helixControllerManager.syncStart();
    System.out.println("controller started");
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

    HelixVcrCluster h = new HelixVcrCluster(cloudConfig, clusterMapConfig, mockClusterMap);
    Thread.sleep(1000);
    System.out.println(h.getAssignedPartitionIds());
    System.out.println("Hello World done!"); //Display the string.
  }
}
