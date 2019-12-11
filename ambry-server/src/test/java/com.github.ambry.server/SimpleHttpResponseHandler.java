/*
 * Copyright 2016 The Netty Project
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

package com.github.ambry.server;

import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http2.HttpConversionUtil;
import java.io.DataInputStream;


@ChannelHandler.Sharable
public class SimpleHttpResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse httpResponse) throws Exception {
    Integer streamId = httpResponse.headers().getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
    System.out.println("stream id" + streamId);
    DataInputStream dataInputStream = new NettyByteBufDataInputStream(httpResponse.content());
    System.out.println(dataInputStream.readLong());
    AdminResponse adminResponse = AdminResponse.readFrom(dataInputStream);
    System.out.println(adminResponse);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    System.out.println(cause);
  }
}


