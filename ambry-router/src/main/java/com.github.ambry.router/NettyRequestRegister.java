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
package com.github.ambry.router;

import com.github.ambry.network.RequestInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * The callback to be used when requests are created or dropped and needs to be sent out or closed. The operation
 * manager passes this callback to the associated operation class and the operation uses this callback when requests are
 * created and need to be sent out. The callback will then update data structures common to all of the different
 * "Manager" classes. The updates to these data structures are not thread safe and this callback should only be called
 * from the main event loop thread.
 */
class NettyRequestRegister<T> implements RequestRegister<T> {
  @Override
  public List<RequestInfo> getRequestsToSend() {
    return null;
  }

  @Override
  public Set<Integer> getRequestsToDrop() {
    return null;
  }

  @Override
  public void registerRequestToSend(T routerOperation, RequestInfo requestInfo) {

  }

  @Override
  public void registerRequestToDrop(int correlationId) {

  }

  @Override
  public void setRequestsToDrop(Set requestsToDrop) {

  }

  @Override
  public void setRequestsToSend(List requestsToSend) {

  }
}
