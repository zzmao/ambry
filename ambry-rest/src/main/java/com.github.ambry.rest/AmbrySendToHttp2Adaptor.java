/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.github.ambry.rest;

import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.Send;
import com.github.ambry.router.Callback;
import com.github.ambry.utils.ByteBufChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2MultiplexCodec;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Translates Ambry Send objects to the corresponding HTTP/2 frame objects.
 */
public class AmbrySendToHttp2Adaptor extends ChannelOutboundHandlerAdapter {

  public AmbrySendToHttp2Adaptor() {

  }

  /**
   * Handles conversion of {@link Send} to HTTP/2 frames.
   */
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (!(msg instanceof Send)) {
      ctx.write(msg, promise);
    }
    System.out.println("converting send to http2");
    Send send = (Send) msg;
    Http2Headers http2Headers = new DefaultHttp2Headers().method(HttpMethod.POST.asciiName()).scheme("https").path("/");
    DefaultHttp2HeadersFrame headersFrame = new DefaultHttp2HeadersFrame(http2Headers, false);
    ctx.write(headersFrame);
    ByteBufChannel byteBufChannel = new ByteBufChannel();
    try {
      send.writeTo(byteBufChannel);
      int index = 0;
      for (ByteBuf byteBuf : byteBufChannel.getBufs()) {
        DefaultHttp2DataFrame dataFrame =
            new DefaultHttp2DataFrame(byteBuf, index == byteBufChannel.getBufs().size() - 1);
        ctx.write(dataFrame);
        index++;
      }
      ctx.flush();
      promise.setSuccess();
    } catch (IOException e) {
      System.out.println(e);
      promise.setFailure(e);
    }
  }
}
