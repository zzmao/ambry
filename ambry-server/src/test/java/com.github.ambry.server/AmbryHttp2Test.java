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

import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.NettySslHttp2Factory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import java.util.Properties;
import org.junit.After;
import org.junit.Test;


/**
 * Tests for {@link AmbryServerRequests}.
 */
public class AmbryHttp2Test {
  static final String URL2DATA = System.getProperty("url2data", "test data!");

  public AmbryHttp2Test() {
  }

  /**
   * Close the storageManager created.
   */
  @After
  public void after() throws InterruptedException {
  }

  /**
   * Tests that requests are validated based on local store state.
   */
  @Test
  public void clientTest() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("ssl.endpoint.identification.algorithm", "HTTPS");
    properties.setProperty("ssl.keystore.type", "PKCS12");
    properties.setProperty("ssl.keystore.path", "/Users/zemao/ambry/identity.p12");
    properties.setProperty("ssl.keystore.password", "work_around_jdk-6879539");
    properties.setProperty("ssl.key.password", "work_around_jdk-6879539");
    properties.setProperty("ssl.truststore.path", "/etc/riddler/cacerts");
    properties.setProperty("ssl.truststore.password", "changeit");

    SSLConfig sslConfig = new SSLConfig(new VerifiableProperties(properties));

    EventLoopGroup workerGroup = new NioEventLoopGroup();

    try {
      // Configure the client.
      Bootstrap b = new Bootstrap();
      b.group(workerGroup);
      b.channel(NioSocketChannel.class);
      b.option(ChannelOption.SO_KEEPALIVE, true);
      b.remoteAddress("zemao-mn1.linkedin.biz", 1173);
      b.handler(new NettyStorageClientChannelInitializer(new NettySslHttp2Factory(sslConfig), "zemao-mn1.linkedin.biz",
          8443));

      // Start the client.
      Channel channel = b.connect().syncUninterruptibly().channel();
      System.out.println("Connected to remote host");

      // Start a child channel.
      ChannelInitializer<Channel> initializer = new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel ch) {
          ChannelPipeline p = ch.pipeline();
          p.addLast("child-client-frame-converter", new Http2StreamFrameToHttpObjectCodec(false));
//          p.addLast("child-client-decompressor", new HttpContentDecompressor());
          p.addLast("child-client-chunk-aggregator", new HttpObjectAggregator(1024 * 1024));
          p.addLast("child-client-response-handler", new SimpleHttpResponseHandler());
        }
      };

      for (int i = 0; i < 2; i++) {
        Http2StreamChannel childChannel =
            new Http2StreamChannelBootstrap(channel).handler(initializer).open().syncUninterruptibly().getNow();
        Http2Headers http2Headers =
            new DefaultHttp2Headers().method(HttpMethod.POST.asciiName()).scheme("https").path("/");
        DefaultHttp2HeadersFrame headersFrame = new DefaultHttp2HeadersFrame(http2Headers, true);
        childChannel.write(headersFrame);
        childChannel.flush();
        System.out.println(childChannel.stream().id());
      }

      Thread.sleep(5000);

      // Wait until the connection is closed.
      channel.close().syncUninterruptibly();
    } finally {
      workerGroup.shutdownGracefully();
    }
  }
}
