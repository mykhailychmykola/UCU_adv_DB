package com.DB.kolya;

import java.nio.ByteBuffer;
import java.util.UUID;

public class MyUtil {
    private static final int SIZE_IN_BYTES = 16;

    public static byte[] toBytes(final UUID uuid) {
        final ByteBuffer buffer = ByteBuffer.allocate(SIZE_IN_BYTES);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }

    public static UUID fromBytes(final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final long ms = buffer.getLong();
        final long ls = buffer.getLong();
        return new UUID(ms, ls);
    }

}

