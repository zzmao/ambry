/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A {@link WritableByteChannel} that stores the bytes written into it in a {@link ByteBuf}.
 */
public class ByteBufChannel implements WritableByteChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private CompositeByteBuf compositeBuffer = ByteBufAllocator.DEFAULT.compositeBuffer();

  /**
   * Gets the {@link CompositeByteBuf} that is being used to receive writes.
   * @return the {@link ByteBuf} that is receives writes to this channel.
   */
  public ByteBuf getBuf() {
    return compositeBuffer;
  }

  /**
   * This object needs to be instantiated with a {@link ByteBuf} that is provided by the caller. The maximum
   * number of bytes that can be written into the {@code buffer} is determined by {@code buffer.remaining()}.
   */
  public ByteBufChannel() {

  }

  /**
   * Copies bytes from {@code src} into the bufs.
   * @param src the source {@link ByteBuf} to copy bytes from.
   * @return the number of bytes copied.
   * @throws ClosedChannelException if the channel is closed when this function was called.
   */
  @Override
  public int write(ByteBuffer src) throws ClosedChannelException {
    int size = src.remaining();
    compositeBuffer.addComponent(true, Unpooled.wrappedBuffer(src));
    src.position(src.limit());
    return size;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close() {
    channelOpen.set(false);
  }
}
