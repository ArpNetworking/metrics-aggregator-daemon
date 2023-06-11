/*
 * Copyright 2023 Inscope Metrics Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arpnetworking.http;

import akka.actor.ClassicActorSystemProvider;
import akka.http.scaladsl.model.ContentType;
import akka.http.scaladsl.model.HttpEntity;
import akka.http.scaladsl.model.HttpMessage;
import akka.http.scaladsl.model.RequestEntity;
import akka.http.scaladsl.model.UniversalEntity;
import akka.stream.Materializer;
import akka.stream.scaladsl.Flow;
import akka.stream.scaladsl.Source;
import akka.util.ByteString;
import scala.Option;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.OptionalLong;
import java.util.concurrent.CompletionStage;

/**
 * Wraps an {@link RequestEntity} and counts the number of bytes read.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class CountingEntityWrapper implements RequestEntity {

    private final RequestEntity _wrapped;

    /**
     * Public constructor.
     *
     * @param wrapped The entity to wrap.
     */
    public CountingEntityWrapper(final RequestEntity wrapped) {
        _wrapped = wrapped;
    }

    public long getBytesConsumed() {
        return _wrapped.getContentLengthOption().orElse(0);
    }


    @Override
    public RequestEntity withContentType(final ContentType contentType) {
        return _wrapped.withContentType(contentType);
    }

    @Override
    public boolean isKnownEmpty() {
        return _wrapped.isKnownEmpty();
    }

    @Override
    public RequestEntity withSizeLimit(final long maxBytes) {
        return _wrapped.withSizeLimit(maxBytes);
    }

    @Override
    public RequestEntity withoutSizeLimit() {
        return _wrapped.withoutSizeLimit();
    }

    @Override
    public akka.http.javadsl.model.ContentType getContentType() {
        return _wrapped.getContentType();
    }

    @Override
    public akka.stream.javadsl.Source<ByteString, Object> getDataBytes() {
        return RequestEntity.super.getDataBytes();
    }

    @Override
    public OptionalLong getContentLengthOption() {
        return RequestEntity.super.getContentLengthOption();
    }

    @Override
    public boolean isCloseDelimited() {
        return RequestEntity.super.isCloseDelimited();
    }

    @Override
    public boolean isIndefiniteLength() {
        return RequestEntity.super.isIndefiniteLength();
    }

    @Override
    public boolean isStrict() {
        return RequestEntity.super.isStrict();
    }

    @Override
    public boolean isDefault() {
        return RequestEntity.super.isDefault();
    }

    @Override
    public boolean isChunked() {
        return RequestEntity.super.isChunked();
    }

    @Override
    public CompletionStage<akka.http.javadsl.model.HttpEntity.Strict> toStrict(
            final long timeoutMillis,
            final Materializer materializer) {
        return RequestEntity.super.toStrict(timeoutMillis, materializer);
    }

    @Override
    public CompletionStage<akka.http.javadsl.model.HttpEntity.Strict> toStrict(
            final long timeoutMillis,
            final long maxBytes,
            final Materializer materializer) {
        return RequestEntity.super.toStrict(timeoutMillis, maxBytes, materializer);
    }

    @Override
    public CompletionStage<akka.http.javadsl.model.HttpEntity.Strict> toStrict(
            final long timeoutMillis,
            final ClassicActorSystemProvider system) {
        return RequestEntity.super.toStrict(timeoutMillis, system);
    }

    @Override
    public CompletionStage<akka.http.javadsl.model.HttpEntity.Strict> toStrict(
            final long timeoutMillis,
            final long maxBytes,
            final ClassicActorSystemProvider system) {
        return RequestEntity.super.toStrict(timeoutMillis, maxBytes, system);
    }

    @Override
    public HttpEntity withContentType(final akka.http.javadsl.model.ContentType contentType) {
        return RequestEntity.super.withContentType(contentType);
    }

    @Override
    public ContentType contentType() {
        return _wrapped.contentType();
    }

    @Override
    public Option<Object> contentLengthOption() {
        return _wrapped.contentLengthOption();
    }

    @Override
    public Source<ByteString, Object> dataBytes() {
        return _wrapped.dataBytes();
    }

    @Override
    public Future<HttpEntity.Strict> toStrict(final FiniteDuration timeout, final Materializer fm) {
        return RequestEntity.super.toStrict(timeout, fm);
    }

    @Override
    public Future<HttpEntity.Strict> toStrict(final FiniteDuration timeout, final long maxBytes, final Materializer fm) {
        return RequestEntity.super.toStrict(timeout, maxBytes, fm);
    }

    @Override
    public HttpMessage.DiscardedEntity discardBytes(final Materializer mat) {
        return RequestEntity.super.discardBytes(mat);
    }

    @Override
    public HttpMessage.DiscardedEntity discardBytes(final ClassicActorSystemProvider system) {
        return RequestEntity.super.discardBytes(system);
    }

    @Override
    public RequestEntity transformDataBytes(final Flow<ByteString, ByteString, Object> transformer) {
        return _wrapped.transformDataBytes(transformer);
    }

    @Override
    public UniversalEntity transformDataBytes(
            final long newContentLength,
            final Flow<ByteString, ByteString, Object> transformer) {
        return RequestEntity.super.transformDataBytes(newContentLength, transformer);
    }
}
