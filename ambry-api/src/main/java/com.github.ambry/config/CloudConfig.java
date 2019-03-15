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
package com.github.ambry.config;

/**
 * The configs for cloud related configurations.
 */
public class CloudConfig {

  @Config("vcr.cluster.zk.connect.string")
  @Default("")
  public final String vcrClusterZkConnectString;

  /**
   * The name of the associated vcr cluster for this node.
   */
  @Config("vcr.cluster.name")
  public final String vcrClusterName;

  /**
   * The name of the associated datacenter for this node.
   */
  @Config("vcr.datacenter.name")
  public final String vcrDatacenterName;

  /**
   * The ssl port number associated with this node.
   */
  @Config("vcr.ssl.port")
  @Default("null")
  public final Integer vcrSslPort;

  public CloudConfig(VerifiableProperties verifiableProperties) {
    vcrClusterZkConnectString = verifiableProperties.getString("vcr.cluster.zk.connect.string", "");
    vcrClusterName = verifiableProperties.getString("vcr.cluster.name");
    vcrDatacenterName = verifiableProperties.getString("vcr.datacenter.name");
    vcrSslPort = verifiableProperties.getInteger("vcr.ssl.port", null);
  }
}
