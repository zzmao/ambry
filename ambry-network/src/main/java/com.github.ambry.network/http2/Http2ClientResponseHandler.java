/*
 * Copyright 2020 The Netty Project
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
package com.github.ambry.network.http2;

import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.AttributeKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Process {@link io.netty.handler.codec.http.FullHttpResponse} translated from HTTP/2 frames
 */
@ChannelHandler.Sharable
public class Http2ClientResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
  public static AttributeKey<RequestInfo> REQUEST_INFO = AttributeKey.newInstance("requestInfo");
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  private Queue<ResponseInfo> responseInfoQueue0 = new ConcurrentLinkedQueue<>();
  private Queue<ResponseInfo> responseInfoQueue1 = new ConcurrentLinkedQueue<>();
  private volatile Queue<ResponseInfo> responseInfoQueueToProduce = responseInfoQueue0;

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
    ByteBuf dup = msg.content().retainedDuplicate();
    dup.readLong();
    ResponseInfo responseInfo = new ResponseInfo(ctx.channel().attr(REQUEST_INFO).get(), null, dup);
    getQueueToProduce().add(responseInfo);
    System.out.println("response come: " + responseInfo);
    // TODO: is this a good place to release this channel?
    // Release stream channel
    ctx.channel().parent().attr(Http2MultiplexedChannelPool.HTTP2_MULTIPLEXED_CHANNEL_POOL).get().release(ctx.channel());
  }

  private synchronized Queue<ResponseInfo> getQueueToProduce() {
    return responseInfoQueueToProduce;
  }

  public Queue<ResponseInfo> getQueueToConsume() {
    if (responseInfoQueueToProduce == responseInfoQueue0) {
      return responseInfoQueue1;
    } else {
      return responseInfoQueue0;
    }
  }

  public synchronized void swapQueue() {
    if (responseInfoQueueToProduce == responseInfoQueue0) {
      responseInfoQueueToProduce = responseInfoQueue1;
    } else {
      responseInfoQueueToProduce = responseInfoQueue0;
    }
  }
}
