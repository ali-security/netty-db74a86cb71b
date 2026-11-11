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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.List;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderValues.SNAPPY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    private EmbeddedChannel parent;
    private ChannelHandlerContext ctx;
    private Http2Connection connection;
    private Http2LocalFlowController flowController;
    private final List<ByteBuf> decompressedBuffers = new ArrayList<ByteBuf>();

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
        Http2Headers headers = new DefaultHttp2Headers();
        headers.set(CONTENT_ENCODING, SNAPPY);
        listener.onHeadersRead(ctx, STREAM_ID, headers, 0, false);
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
}
