/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.rest;

import com.github.ambry.commons.JdkSslFactory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.SSLConfig;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import java.io.IOException;
import java.security.GeneralSecurityException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.rest.NettySslFactory.*;


/**
 * An implementation of {@link SSLFactory} that uses Netty's SSL libraries. This has the benefit of using OpenSSL
 * instead of JDK's SSL implementation when the netty-tcnative library is loaded. OpenSSL shows
 * significant performance enhancements over the JDK implementation.
 */
public class NettySslHttp2Factory implements SSLFactory {
  private static final Logger logger = LoggerFactory.getLogger(NettySslFactory.class);
  private final SslContext nettyServerSslContext;
  private final SslContext nettyClientSslContext;
  private final SSLContext jdkSslContext;
  private final String endpointIdentification;

  /**
   * Instantiate a {@link NettySslFactory} from a config.
   * @param sslConfig the {@link SSLConfig} to use.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public NettySslHttp2Factory(SSLConfig sslConfig) throws GeneralSecurityException, IOException {
    nettyServerSslContext = getServerSslContext(sslConfig);
    nettyClientSslContext = getClientSslContext(sslConfig);
    // Netty's OpenSsl based implementation does not use the JDK SSLContext so we have to fall back to the JDK based
    // factory to support this method.
    jdkSslContext = new JdkSslFactory(sslConfig).getSSLContext();

    this.endpointIdentification =
        sslConfig.sslEndpointIdentificationAlgorithm.isEmpty() ? null : sslConfig.sslEndpointIdentificationAlgorithm;
  }

  @Override
  public SSLEngine createSSLEngine(String peerHost, int peerPort, Mode mode) {
    SslContext context = mode == Mode.CLIENT ? nettyClientSslContext : nettyServerSslContext;
    SSLEngine sslEngine = context.newEngine(ByteBufAllocator.DEFAULT, peerHost, peerPort);

    if (mode == Mode.CLIENT) {
      SSLParameters sslParams = sslEngine.getSSLParameters();
      sslParams.setEndpointIdentificationAlgorithm(endpointIdentification);
      sslEngine.setSSLParameters(sslParams);
    }
    return sslEngine;
  }

  @Override
  public SSLContext getSSLContext() {
    return jdkSslContext;
  }

  /**
   * @param config the {@link SSLConfig}
   * @return a configured {@link SslContext} object for a client.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  static SslContext getServerSslContext(SSLConfig config) throws GeneralSecurityException, IOException {
    logger.info("Using {} provider for server SslContext", SslContext.defaultServerProvider());
    return SslContextBuilder.forServer(NettySslFactory.getKeyManagerFactory(config))
        .trustManager(getTrustManagerFactory(config))
        .protocols(NettySslFactory.getEnabledProtocols(config))
        .clientAuth(NettySslFactory.getClientAuth(config))
        /* NOTE: the cipher filter may not include all ciphers required by the HTTP/2 specification.
         * Please refer to the HTTP/2 specification for cipher requirements. */
        .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
        .applicationProtocolConfig(new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.ALPN,
            // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
            // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT, ApplicationProtocolNames.HTTP_2))
        .build();
  }

  /**
   * @param config the {@link SSLConfig}
   * @return a configured {@link SslContext} object for a server.
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public static SslContext getClientSslContext(SSLConfig config) throws GeneralSecurityException, IOException {
    logger.info("Using {} provider for client SslContext", SslContext.defaultClientProvider());
    return SslContextBuilder.forClient()
        .keyManager(getKeyManagerFactory(config))
        .trustManager(getTrustManagerFactory(config))
        .protocols(getEnabledProtocols(config))
        /* NOTE: the cipher filter may not include all ciphers required by the HTTP/2 specification.
         * Please refer to the HTTP/2 specification for cipher requirements. */
        .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
        .applicationProtocolConfig(new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.ALPN,
            // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
            // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT, ApplicationProtocolNames.HTTP_2))
        .build();
  }
}
