/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.network.RequestInfo;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.ssl.SslHandler;


/**
 * A {@link ChannelInitializer} to be used with {@link Http2BlockingChannel}. Calling {@link #initChannel(SocketChannel)}
 * adds the necessary handlers to a channel's pipeline so that it may handle requests.
 */
public class ChannelPipelineInitializer extends AbstractChannelPoolHandler {
  private final SSLConfig sslConfig;
  private final String host;
  private final int port;

  /**
   * Construct a {@link ChannelPipelineInitializer}.
   * @param sslConfig the {@link SSLFactory} to use for generating {@link javax.net.ssl.SSLEngine} instances,
   *                   or {@code null} if SSL is not enabled in this pipeline.
   */
  public ChannelPipelineInitializer(SSLConfig sslConfig, RequestInfo requestInfo) {
    this.sslConfig = sslConfig;
    this.host = requestInfo.getHost();
    this.port = requestInfo.getPort().getPort();
  }

  @Override
  public void channelCreated(Channel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();
    SSLFactory sslFactory = new NettySslHttp2Factory(sslConfig);
    if (sslFactory == null) {
      throw new IllegalArgumentException("ssl factory shouldn't be null");
    }
    SslHandler sslHandler = new SslHandler(sslFactory.createSSLEngine(host, port, SSLFactory.Mode.CLIENT));
    pipeline.addLast(sslHandler);
    pipeline.addLast(Http2FrameCodecBuilder.forClient().initialSettings(Http2Settings.defaultSettings()).build());
    pipeline.addLast(new Http2MultiplexHandler(new ChannelInboundHandlerAdapter()));
  }
}

