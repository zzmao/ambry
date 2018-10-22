/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.github.ambry.config.FrontendConfig;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.utils.RejectThrottler;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * A class to manage requests based on request context.
 */
public class QuotaManager {
  protected final Map<RestMethod, RejectThrottler> quotaMap = new HashMap<>();
  protected final JSONObject quota;

  public QuotaManager(FrontendConfig frontendConfig) {
    try {
      quota = new JSONObject(frontendConfig.restRequestQuota);
    } catch (JSONException ex) {
      throw new IllegalStateException("Invalid config value: " + frontendConfig.restRequestQuota, ex);
    }
    for (RestMethod restMethod : RestMethod.values()) {
      quotaMap.put(restMethod, new RejectThrottler(quota.getInt(restMethod.name())));
    }
  }

  /**
   * Return {@code true} if throttling is required.
   * @param restRequest provides the information.
   */
  public boolean shouldThrottle(RestRequest restRequest) {
    return quotaMap.get(restRequest.getRestMethod()).shouldThrottle(1);
  }
}
