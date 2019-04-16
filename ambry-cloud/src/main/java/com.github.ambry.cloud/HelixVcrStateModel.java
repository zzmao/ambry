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

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link StateModel} to use when the VCR participants register to Helix. The methods are callbacks
 * that get called within a participant whenever its state changes in Helix.
 */
@StateModelInfo(initialState = "OFFLINE", states = {"LEADER", "STANDBY"})
public class HelixVcrStateModel extends StateModel {
  private Logger logger = LoggerFactory.getLogger(getClass());
  private HelixVcrCluster helixVcrCluster;

  HelixVcrStateModel(HelixVcrCluster helixVcrCluster) {
    this.helixVcrCluster = helixVcrCluster;
  }

  @Transition(to = "STANDBY", from = "OFFLINE")
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    logger.info("Becoming STANDBY from OFFLINE");
  }

  @Transition(to = "LEADER", from = "STANDBY")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
    helixVcrCluster.addPartition(message.getPartitionName());
    logger.info("Becoming LEADER from STANDBY");
  }

  @Transition(to = "STANDBY", from = "LEADER")
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    helixVcrCluster.removePartition(message.getPartitionName());
    logger.info("Becoming STANDBY from LEADER");
  }

  @Transition(to = "OFFLINE", from = "STANDBY")
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    logger.info("Becoming OFFLINE from STANDBY");
  }

  @Transition(to = "OFFLINE", from = "LEADER")
  public void onBecomeOfflineFromLeader(Message message, NotificationContext context) {
    helixVcrCluster.removePartition(message.getPartitionName());
    logger.info("Becoming OFFLINE from LEADER");
  }

  @Override
  public void reset() {
    // no op
  }
}
