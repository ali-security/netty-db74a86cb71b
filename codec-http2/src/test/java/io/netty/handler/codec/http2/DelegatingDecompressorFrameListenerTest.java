/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.netty.handler.codec.http2;

import com.aayushatharva.brotli4j.encoder.BrotliOutputStream;
import com.aayushatharva.brotli4j.encoder.Encoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Brotli;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderValues.BR;
import static io.netty.handler.codec.http.HttpHeaderValues.GZIP;
import static io.netty.handler.codec.http.HttpHeaderValues.SNAPPY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DelegatingDecompressorFrameListener} which verify that all decompressed buffers are released,
 * even if the listener or the flow controller throws while the decompressed data is dispatched.
 */
public class DelegatingDecompressorFrameListenerTest {

    private static final int STREAM_ID = 3;
    // Both payloads must be larger then the minimal compressible length of the snappy framing format so each of
    // them ends up in its own COMPRESSED_DATA chunk, which the decompressor will emit as its own ByteBuf.
    private static final String FIRST_CHUNK = "netty netty netty netty netty netty";
    private static final String SECOND_CHUNK = "leak leak leak leak leak leak leak!";
    private static final int MAX_ALLOCATION = 64 * 1024;

    private EmbeddedChannel parent;
    private ChannelHandlerContext ctx;
    private Http2Connection connection;
    private Http2LocalFlowController flowController;
    private final List<ByteBuf> decompressedBuffers = new ArrayList<ByteBuf>();
    private long decoderOutputBytes;

    @BeforeEach
    public void setUp() throws Http2Exception {
        parent = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        // Use an unpooled allocator so a released buffer is not recycled and its reference count stays observable.
        parent.config().setAllocator(UnpooledByteBufAllocator.DEFAULT);
        ctx = parent.pipeline().firstContext();
        connection = new DefaultHttp2Connection(true);
        flowController = mock(Http2LocalFlowController.class);
        connection.local().flowController(flowController);
        connection.remote().createStream(STREAM_ID, false);
    }

    @AfterEach
    public void tearDown() {
        for (int i = 0; i < decompressedBuffers.size(); i++) {
            ByteBuf buf = decompressedBuffers.get(i);
            if (buf.refCnt() > 0) {
                buf.release(buf.refCnt());
            }
        }
        decompressedBuffers.clear();
        parent.finishAndReleaseAll();
    }

    @Test
    public void testDecompressedBuffersReleasedWhenListenerThrows() throws Exception {
        Http2FrameListener delegate = new Http2FrameAdapter() {
            @Override
            public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                    boolean endOfStream) throws Http2Exception {
                throw new Http2Exception(Http2Error.INTERNAL_ERROR, "listener failure");
            }
        };
        assertNoLeakWhenDispatchFails(delegate);
    }

    @Test
    public void testDecompressedBuffersReleasedWhenListenerThrowsRuntimeException() throws Exception {
        Http2FrameListener delegate = new Http2FrameAdapter() {
            @Override
            public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                    boolean endOfStream) throws Http2Exception {
                throw new IllegalStateException("listener failure");
            }
        };
        assertNoLeakWhenDispatchFails(delegate);
    }

    @Test
    public void testDecompressedBuffersReleasedWhenFlowControllerThrows() throws Exception {
        when(flowController.consumeBytes(any(Http2Stream.class), anyInt()))
                .thenThrow(new Http2Exception(Http2Error.INTERNAL_ERROR, "flow controller failure"));
        assertNoLeakWhenDispatchFails(new Http2FrameAdapter());
    }

    @Test
    public void testDecompressedBuffersReleasedOnSuccess() throws Exception {
        final List<String> dispatched = new ArrayList<String>();
        Http2FrameListener delegate = new Http2FrameAdapter() {
            @Override
            public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                    boolean endOfStream) throws Http2Exception {
                dispatched.add(data.toString(CharsetUtil.US_ASCII));
                return data.readableBytes() + padding;
            }
        };
        DelegatingDecompressorFrameListener listener = newListener(delegate);
        initStream(listener);

        ByteBuf compressed = snappyFrames(FIRST_CHUNK, SECOND_CHUNK);
        try {
            listener.onDataRead(ctx, STREAM_ID, compressed, 0, false);
        } finally {
            releaseIfNeeded(compressed);
        }

        assertEquals(2, dispatched.size());
        assertEquals(FIRST_CHUNK, dispatched.get(0));
        assertEquals(SECOND_CHUNK, dispatched.get(1));
        assertAllDecompressedBuffersReleased();
    }

    @Test
    public void testNegativeMaxAllocationRejected() {
        assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                new DelegatingDecompressorFrameListener(connection, new Http2FrameAdapter(), -1);
            }
        });
    }

    /**
     * A single small brotli {@code DATA} frame that expands to 8 MiB must fail the stream before the decoder
     * materializes more than {@code maxAllocation} decompressed bytes.
     */
    @Test
    public void testBrotliBombRejectedBeforeDecompressingBeyondMaxAllocation() throws Throwable {
        Brotli.ensureAvailability();
        ByteBuf compressed = Unpooled.wrappedBuffer(brotli(new byte[8 * 1024 * 1024]));
        // The whole bomb fits into a single DATA frame.
        assertTrue(compressed.readableBytes() <= Http2CodecUtil.DEFAULT_MAX_FRAME_SIZE);

        RecordingListener delegate = new RecordingListener();
        DelegatingDecompressorFrameListener listener = newBoundedListener(delegate);
        initStream(listener, BR);
        assertDecompressionLimitExceeded(listener, compressed);

        assertTrue(decoderOutputBytes <= MAX_ALLOCATION,
                "decoder produced " + decoderOutputBytes + " bytes, limit is " + MAX_ALLOCATION);
        assertTrue(delegate.received.size() <= MAX_ALLOCATION);
        assertTrue(retainedDecompressedBytes() <= MAX_ALLOCATION);
        closeStreamAndAssertAllDecompressedBuffersReleased();
    }

    @Test
    public void testBrotliWithinMaxAllocationIsDecompressed() throws Throwable {
        Brotli.ensureAvailability();
        byte[] content = new byte[MAX_ALLOCATION];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) ('a' + i % 26);
        }
        ByteBuf compressed = Unpooled.wrappedBuffer(brotli(content));
        int half = compressed.readableBytes() / 2;
        ByteBuf first = compressed.copy(0, half);
        ByteBuf second = compressed.copy(half, compressed.readableBytes() - half);
        compressed.release();

        RecordingListener delegate = new RecordingListener();
        DelegatingDecompressorFrameListener listener = newBoundedListener(delegate);
        initStream(listener, BR);
        try {
            listener.onDataRead(ctx, STREAM_ID, first, 0, false);
            listener.onDataRead(ctx, STREAM_ID, second, 0, true);
        } finally {
            releaseIfNeeded(first);
            releaseIfNeeded(second);
        }

        assertArrayEquals(content, delegate.received.toByteArray());
        closeStreamAndAssertAllDecompressedBuffersReleased();
    }

    /**
     * Snappy output is produced in chunks of at most 64 KiB, but a stream may carry any number of them. Once the
     * total exceeds {@code maxAllocation} the stream must fail and nothing beyond the limit may be delivered or kept.
     */
    @Test
    public void testSnappyOutputBoundedByMaxAllocation() throws Exception {
        char[] chars = new char[256 * 1024];
        Arrays.fill(chars, 'a');
        ByteBuf compressed = snappyFrames(new String(chars));

        RecordingListener delegate = new RecordingListener();
        DelegatingDecompressorFrameListener listener = newBoundedListener(delegate);
        initStream(listener, SNAPPY);
        assertDecompressionLimitExceeded(listener, compressed);

        assertTrue(delegate.received.size() <= MAX_ALLOCATION);
        assertTrue(retainedDecompressedBytes() <= MAX_ALLOCATION);
        closeStreamAndAssertAllDecompressedBuffersReleased();
    }

    /**
     * The limit applies to the whole stream: frames which each stay below {@code maxAllocation} are delivered until
     * their sum exceeds it.
     */
    @Test
    public void testGzipStreamFailsOnceTotalExceedsMaxAllocation() throws Exception {
        byte[] frameContent = new byte[16 * 1024];
        Arrays.fill(frameContent, (byte) 'a');
        List<ByteBuf> frames = gzipFrames(frameContent, 8);

        RecordingListener delegate = new RecordingListener();
        DelegatingDecompressorFrameListener listener = newBoundedListener(delegate);
        initStream(listener, GZIP);
        Http2Exception failure = null;
        try {
            for (int i = 0; i < frames.size(); i++) {
                try {
                    listener.onDataRead(ctx, STREAM_ID, frames.get(i), 0, false);
                } catch (Http2Exception e) {
                    failure = e;
                    break;
                }
            }
        } finally {
            for (int i = 0; i < frames.size(); i++) {
                releaseIfNeeded(frames.get(i));
            }
        }

        assertTrue(failure != null, "stream did not fail although " + frames.size() * frameContent.length +
                " bytes were decompressed, limit is " + MAX_ALLOCATION);
        assertDecompressionLimitCause(failure);
        assertTrue(delegate.received.size() > 0);
        assertTrue(delegate.received.size() <= MAX_ALLOCATION);
        closeStreamAndAssertAllDecompressedBuffersReleased();
    }

    /**
     * Feeds a data frame that decompresses into more then one buffer while the dispatching of the first decompressed
     * buffer fails. All buffers that were extracted from the decompressor must be released in this case, otherwise a
     * remote peer could exhaust the memory of the JVM.
     */
    private void assertNoLeakWhenDispatchFails(Http2FrameListener delegate) throws Exception {
        final DelegatingDecompressorFrameListener listener = newListener(delegate);
        initStream(listener);

        final ByteBuf compressed = snappyFrames(FIRST_CHUNK, SECOND_CHUNK);
        try {
            assertThrows(Http2Exception.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    listener.onDataRead(ctx, STREAM_ID, compressed, 0, false);
                }
            });
        } finally {
            releaseIfNeeded(compressed);
        }

        assertAllDecompressedBuffersReleased();
    }

    private void assertAllDecompressedBuffersReleased() {
        // Ensure the payload was really decompressed into more then one buffer, otherwise the test would not
        // exercise the case in which a buffer was extracted from the decompressor but never dispatched.
        assertEquals(2, decompressedBuffers.size());
        for (int i = 0; i < decompressedBuffers.size(); i++) {
            assertEquals(0, decompressedBuffers.get(i).refCnt(),
                    "decompressed buffer " + i + " was not released");
        }
    }

    private void initStream(DelegatingDecompressorFrameListener listener) throws Http2Exception {
        initStream(listener, SNAPPY);
    }

    private void initStream(DelegatingDecompressorFrameListener listener, CharSequence contentEncoding)
            throws Http2Exception {
        Http2Headers headers = new DefaultHttp2Headers();
        headers.set(CONTENT_ENCODING, contentEncoding);
        listener.onHeadersRead(ctx, STREAM_ID, headers, 0, false);
    }

    /**
     * Creates a listener limited to {@link #MAX_ALLOCATION} decompressed bytes per stream which records every buffer
     * the decoder produces, before any other handler of the decompressor could drop it.
     */
    private DelegatingDecompressorFrameListener newBoundedListener(Http2FrameListener delegate) {
        return new DelegatingDecompressorFrameListener(connection, delegate, MAX_ALLOCATION) {
            @Override
            protected EmbeddedChannel newContentDecompressor(ChannelHandlerContext ctx, CharSequence contentEncoding)
                    throws Http2Exception {
                EmbeddedChannel decompressor = super.newContentDecompressor(ctx, contentEncoding);
                if (decompressor != null) {
                    decompressor.pipeline().addAfter(decompressor.pipeline().firstContext().name(), null,
                            new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                    if (msg instanceof ByteBuf) {
                                        decompressedBuffers.add((ByteBuf) msg);
                                        decoderOutputBytes += ((ByteBuf) msg).readableBytes();
                                    }
                                    ctx.fireChannelRead(msg);
                                }
                            });
                }
                return decompressor;
            }
        };
    }

    private void assertDecompressionLimitExceeded(final DelegatingDecompressorFrameListener listener,
                                                  final ByteBuf compressed) {
        Http2Exception e;
        try {
            e = assertThrows(Http2Exception.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    listener.onDataRead(ctx, STREAM_ID, compressed, 0, false);
                }
            });
        } finally {
            releaseIfNeeded(compressed);
        }
        assertDecompressionLimitCause(e);
    }

    private static void assertDecompressionLimitCause(Throwable cause) {
        assertTrue(cause instanceof Http2Exception.StreamException, "unexpected exception: " + cause);
        Throwable rootCause = cause;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        assertTrue(rootCause instanceof DecompressionException, "unexpected root cause: " + rootCause);
        assertTrue(rootCause.getMessage().contains("maximum size"), rootCause.getMessage());
    }

    /**
     * Returns the number of decompressed bytes which are still referenced, e.g. queued in the decompressor.
     */
    private long retainedDecompressedBytes() {
        long retained = 0;
        for (int i = 0; i < decompressedBuffers.size(); i++) {
            ByteBuf buf = decompressedBuffers.get(i);
            if (buf.refCnt() > 0) {
                retained += buf.readableBytes();
            }
        }
        return retained;
    }

    private void closeStreamAndAssertAllDecompressedBuffersReleased() {
        // Removing the stream releases everything which is still held by its decompressor.
        connection.stream(STREAM_ID).close();
        for (int i = 0; i < decompressedBuffers.size(); i++) {
            assertEquals(0, decompressedBuffers.get(i).refCnt(),
                    "decompressed buffer " + i + " was not released");
        }
    }

    private DelegatingDecompressorFrameListener newListener(Http2FrameListener delegate) {
        return new DelegatingDecompressorFrameListener(connection, delegate) {
            @Override
            protected EmbeddedChannel newContentDecompressor(ChannelHandlerContext ctx, CharSequence contentEncoding)
                    throws Http2Exception {
                EmbeddedChannel decompressor = super.newContentDecompressor(ctx, contentEncoding);
                if (decompressor != null) {
                    // Keep track of every buffer that is produced by the decompressor so we can verify later on
                    // that all of them were released.
                    decompressor.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                            if (msg instanceof ByteBuf) {
                                decompressedBuffers.add((ByteBuf) msg);
                            }
                            ctx.fireChannelRead(msg);
                        }
                    });
                }
                return decompressor;
            }
        };
    }

    private static void releaseIfNeeded(ByteBuf buf) {
        if (buf.refCnt() > 0) {
            buf.release(buf.refCnt());
        }
    }

    private static ByteBuf snappyFrames(String... chunks) {
        EmbeddedChannel encoder = new EmbeddedChannel(new SnappyFrameEncoder());
        ByteBuf compressed = Unpooled.buffer();
        try {
            for (int i = 0; i < chunks.length; i++) {
                encoder.writeOutbound(Unpooled.copiedBuffer(chunks[i], CharsetUtil.US_ASCII));
                for (;;) {
                    ByteBuf part = encoder.readOutbound();
                    if (part == null) {
                        break;
                    }
                    compressed.writeBytes(part);
                    part.release();
                }
            }
        } finally {
            encoder.finishAndReleaseAll();
        }
        return compressed;
    }

    private static byte[] brotli(byte[] data) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        BrotliOutputStream brotliOs = new BrotliOutputStream(os, new Encoder.Parameters().setQuality(4));
        brotliOs.write(data);
        brotliOs.close();
        return os.toByteArray();
    }

    /**
     * Compresses {@code content} {@code count} times into one gzip stream, flushing after each copy so that every
     * returned frame decompresses to exactly {@code content}.
     */
    private static List<ByteBuf> gzipFrames(byte[] content, int count) {
        EmbeddedChannel encoder = new EmbeddedChannel(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
        List<ByteBuf> frames = new ArrayList<ByteBuf>(count);
        try {
            for (int i = 0; i < count; i++) {
                encoder.writeOutbound(Unpooled.wrappedBuffer(content));
                ByteBuf frame = Unpooled.buffer();
                for (;;) {
                    ByteBuf part = encoder.readOutbound();
                    if (part == null) {
                        break;
                    }
                    frame.writeBytes(part);
                    part.release();
                }
                frames.add(frame);
            }
        } finally {
            encoder.finishAndReleaseAll();
        }
        return frames;
    }

    /**
     * Records all decompressed data which is delivered to the application.
     */
    private static final class RecordingListener extends Http2FrameAdapter {
        final ByteArrayOutputStream received = new ByteArrayOutputStream();

        @Override
        public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                boolean endOfStream) {
            int readable = data.readableBytes();
            byte[] bytes = new byte[readable];
            data.readBytes(bytes);
            received.write(bytes, 0, bytes.length);
            return readable + padding;
        }
    }
}
