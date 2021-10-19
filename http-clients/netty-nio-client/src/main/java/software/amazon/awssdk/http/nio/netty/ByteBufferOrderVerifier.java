package software.amazon.awssdk.http.nio.netty;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ByteBufferOrderVerifier {
    private static byte[] expectedData;
    private final String name;
    private final AtomicInteger i = new AtomicInteger(0);
    private final AtomicBoolean reportedBadBoy = new AtomicBoolean(false);

    public ByteBufferOrderVerifier(String name) {
        this.name = name;
    }

    public static void initialize(byte[] expectedData) {
        ByteBufferOrderVerifier.expectedData = expectedData;
    }

    public void feed(ByteBuffer data) {
        data.mark();
        while (data.hasRemaining() && i.get() < expectedData.length) {
            byte b = data.get();

            int currentI = i.getAndIncrement();
            if (expectedData[currentI] != b) {
                reportBadBoy("Bad Boy: " + name + " at index " + i + "(expected " + b + " found " + expectedData[currentI] + ")");
            }
        }
        data.reset();
    }

    private void reportBadBoy(String x) {
        if (reportedBadBoy.compareAndSet(false, true)) {
            System.out.println(x);
        }
    }
}
