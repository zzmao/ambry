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

package com.github.ambry.server;

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.rest.StorageServerNettyFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.ssl.SslHandler;


/**
 * A {@link ChannelInitializer} to be used with {@link StorageServerNettyFactory}. Calling {@link #initChannel(SocketChannel)} adds
 * the necessary handlers to a channel's pipeline so that it may handle requests.
 */
public class NettyStorageClientChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final SSLFactory sslFactory;
  String host;
  int port;

  /**
   * Construct a {@link NettyStorageClientChannelInitializer}.
   * @param sslFactory the {@link SSLFactory} to use for generating {@link javax.net.ssl.SSLEngine} instances,
   *                   or {@code null} if SSL is not enabled in this pipeline.
   */
  public NettyStorageClientChannelInitializer(SSLFactory sslFactory, String host, int port) {
    this.host = host;
    this.port = port;
    this.sslFactory = sslFactory;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    // If channel handler implementations are not annotated with @Sharable, Netty creates a new instance of every class
    // in the pipeline for every connection.
    // i.e. if there are a 1000 active connections there will be a 1000 NettyMessageProcessor instances.
    ChannelPipeline pipeline = ch.pipeline();
    // connection stats handler to track connection related metrics
//    pipeline.addLast("connectionStatsHandler", connectionStatsHandler);
    // if SSL is enabled, add an SslHandler before the HTTP codec
    if (sslFactory == null) {
      throw new IllegalArgumentException("ssl factory shouldn't be null");
    }
    SslHandler sslHandler = new SslHandler(sslFactory.createSSLEngine(host, port, SSLFactory.Mode.CLIENT));
    pipeline.addLast("sslHandler", sslHandler);
    pipeline.addLast(Http2FrameCodecBuilder.forClient().build());
    pipeline.addLast("processor", new Http2MultiplexHandler(new DummyChildHandler()));
  }

  public class DummyChildHandler extends ChannelInboundHandlerAdapter {

    public DummyChildHandler() {
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {

    }
  }
}

