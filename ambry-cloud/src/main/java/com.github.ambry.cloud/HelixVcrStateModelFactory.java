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

import com.github.ambry.clustermap.AmbryStateModel;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;


/**
 * {@link HelixVcrStateModelFactory} to generate VCR Cluster for static partition assignment.
 */

public class HelixVcrStateModelFactory extends StateModelFactory<StateModel> {
  HelixVcrCluster helixVcrCluster;

  public HelixVcrStateModelFactory(HelixVcrCluster helixVcrCluster) {
    this.helixVcrCluster = helixVcrCluster;
  }

  /**
   * Create and return an instance of {@link AmbryStateModel}
   * @param resourceName the resource name for which this state model is being created.
   * @param partitionName the partition name for which this state model is being created.
   *
   * @return an instance of {@link AmbryStateModel}.
   */
  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    System.out.println("state model created");
    System.out.println(resourceName);
    System.out.println(partitionName);
    return new HelixVcrStateModel(helixVcrCluster);
  }
}


