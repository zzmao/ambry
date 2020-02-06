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
  private final ReentrantLock bufferLock = new ReentrantLock();
  private final List<ByteBuf> bufs;

  /**
   * Gets the {@link ByteBuf} that is being used to receive writes.
   * @return the {@link ByteBuf} that is receives writes to this channel.
   */
  public List<ByteBuf> getBufs() {
    return bufs;
  }

  /**
   * This object needs to be instantiated with a {@link ByteBuf} that is provided by the caller. The maximum
   * number of bytes that can be written into the {@code buffer} is determined by {@code buffer.remaining()}.
   */
  public ByteBufChannel() {
    this.bufs = new ArrayList<>();
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Copies bytes from {@code src} into the {@link ByteBuf} ({@code buffer}) backing this channel. The number of
   * bytes copied is the minimum of {@code src.remaining()} and {@code buffer.remaining()}.
   * @param src the source {@link ByteBuf} to copy bytes from.
   * @return the number of bytes copied.
   * @throws ClosedChannelException if the channel is closed when this function was called.
   */
  @Override
  public int write(ByteBuffer src) throws ClosedChannelException {
    bufs.add(Unpooled.wrappedBuffer(src));
    return src.remaining();
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
