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
package com.github.ambry.rest;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.Send;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link NetworkClient} that provides a method for sending a list of requests in the form of {@link Send} to a host:port,
 * and receive responses for sent requests. Requests that come in via {@link #sendAndPoll(List, Set, int)} call,
 * that could not be immediately sent are queued, and an attempt will be made in subsequent invocations of the call (or
 * until they time out).
 * (Note: We will empirically determine whether, rather than queueing a request,
 * a request should be failed if connections could not be checked out if pool limit for its hostPort has been reached
 * and all connections to the hostPort are unavailable).
 *
 * This class is not thread safe.
 */
public class Http2NetworkClient implements NetworkClient {
  private static final Logger logger = LoggerFactory.getLogger(Http2NetworkClient.class);
  private EventLoopGroup eventLoopGroup;
  private final ChannelPoolMap<RequestInfo, ChannelPool> pools;
  private Http2ClientResponseHandler http2ClientResponseHandler;

  public Http2NetworkClient(SSLConfig sslConfig) {
    this.eventLoopGroup = new NioEventLoopGroup();
    this.pools = new Http2ChannelPoolMap(sslConfig, eventLoopGroup);
    this.http2ClientResponseHandler = new Http2ClientResponseHandler();
  }

  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {

    for (RequestInfo requestInfo : requestsToSend) {
      this.pools.get(requestInfo).acquire().addListener((GenericFutureListener<Future<Channel>>) future -> {
        if (future.isSuccess()) {
          Channel streamChannel = future.getNow();
          streamChannel.pipeline().addLast(http2ClientResponseHandler);
          streamChannel.pipeline().addLast(new AmbrySendToHttp2Adaptor());
          streamChannel.attr(Http2ClientResponseHandler.REQUEST_INFO).set(requestInfo);
          streamChannel.write(requestInfo.getRequest());
        }
      });
    }
    // TODO: close channel for requestsToDrop
    Queue<ResponseInfo> queue = http2ClientResponseHandler.acquireListToConsume();
    List<ResponseInfo> list = new ArrayList<>();
    ResponseInfo responseInfo = queue.poll();
    while (responseInfo != null) {
      list.add(responseInfo);
      responseInfo = queue.poll();
    }
    http2ClientResponseHandler.releaseList();
    return list;
  }

  @Override
  public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode,
      long timeForWarmUp, List<ResponseInfo> responseInfoList) {
    return 0;
  }

  @Override
  public void wakeup() {

  }

  @Override
  public void close() {

  }
}
