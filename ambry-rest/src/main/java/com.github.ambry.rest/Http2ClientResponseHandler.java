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
import com.github.ambry.router.Callback;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.AttributeKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Process {@link io.netty.handler.codec.http.FullHttpResponse} translated from HTTP/2 frames
 */
@ChannelHandler.Sharable
public class Http2ClientResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
  public static AttributeKey<Callback<ByteBuf>> RESPONSE_CALLBACK = AttributeKey.newInstance("responseCallback");
  public static AttributeKey<RequestInfo> REQUEST_INFO = AttributeKey.newInstance("requestInfo");
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  private Queue<ResponseInfo> responseInfoList0 = new ConcurrentLinkedQueue<>();
  private Queue<ResponseInfo> responseInfoList1 = new ConcurrentLinkedQueue<>();
  private volatile Queue<ResponseInfo> responseInfoListToProduce = responseInfoList0;
  private Lock lock = new ReentrantLock();

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
    System.out.println("response come: " + msg.content());
    getListToProduce().add(
        new ResponseInfo(ctx.channel().attr(REQUEST_INFO).get(), null, msg.content().retainedDuplicate()));
    // TODO: is this a good place to release this channel?
    ctx.channel().attr(Http2MultiplexedChannelPool.HTTP2_MULTIPLEXED_CHANNEL_POOL).get().release(ctx.channel());
  }

  private Queue<ResponseInfo> getListToProduce() {
    return responseInfoListToProduce;
  }

  public Queue<ResponseInfo> acquireListToConsume() {
    if (responseInfoListToProduce == responseInfoList0) {
      return responseInfoList1;
    } else {
      return responseInfoList0;
    }
  }

  public void releaseList() {
    if (responseInfoListToProduce == responseInfoList0) {
      responseInfoListToProduce = responseInfoList1;
    } else {
      responseInfoListToProduce = responseInfoList0;
    }
  }
}
