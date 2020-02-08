package com.github.ambry.rest;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.charset.StandardCharsets;


/**
 * Exception thrown when a GOAWAY frame is sent by the service.
 */
class GoAwayException extends IOException {
  private final String message;

  GoAwayException(long errorCode, ByteBuf debugData) {
    this.message = String.format("GOAWAY received from service, requesting this stream be closed. "
            + "Error Code = %d, Debug Data = %s",
        errorCode, debugData.toString(StandardCharsets.UTF_8));
  }

  @Override
  public String getMessage() {
    return message;
  }
}