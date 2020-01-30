package com.github.ambry.rest;

import com.github.ambry.config.SSLConfig;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.rest.ChannelPipelineInitializer;
import com.github.ambry.rest.Http2ClientChannelInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;


public class Http2ChannelPoolMap extends AbstractChannelPoolMap<RequestInfo, ChannelPool> {
  private final EventLoopGroup eventLoopGroup;
  private final SSLConfig sslConfig;

  public Http2ChannelPoolMap(SSLConfig sslConfig, EventLoopGroup eventLoopGroup) {
    this.sslConfig = sslConfig;
    this.eventLoopGroup = eventLoopGroup;
  }

  @Override
  protected ChannelPool newPool(RequestInfo requestInfo) {
    Bootstrap bootstrap = new Bootstrap().group(eventLoopGroup)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .remoteAddress(requestInfo.getHost(), requestInfo.getPort().getPort());

    ChannelPipelineInitializer pipelineInitializer = new ChannelPipelineInitializer(sslConfig, requestInfo);
    return new Http2MultiplexedChannelPool(bootstrap, pipelineInitializer, eventLoopGroup, null, 2);
  }
}
