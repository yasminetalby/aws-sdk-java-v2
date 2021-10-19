package software.amazon.awssdk.http.nio.netty;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ByteBufOrderVerifier {
    private static byte[] expectedData;
    private final String name;
    private final AtomicInteger i = new AtomicInteger(0);
    private final AtomicBoolean reportedBadBoy = new AtomicBoolean(false);
    private volatile boolean headersDone = false;
    private volatile boolean newLineSeen = false;

    public ByteBufOrderVerifier(String name) {
        this.name = name;
    }

    public static void initialize(byte[] expectedData) {
        ByteBufOrderVerifier.expectedData = expectedData;
    }

    public void feed(ByteBuf data) {
        data.markReaderIndex();
        while (data.isReadable()) {
            byte b = data.readByte();

            if (b == '\n') {
                if (newLineSeen) {
                    headersDone = true;
                }
                newLineSeen = true;
            } else {
                newLineSeen = false;
            }

            if (headersDone && i.get() < expectedData.length) {
                int currentI = i.getAndIncrement();
                if (expectedData[currentI] != b) {
                    reportBadBoy("Bad Boy: " + name + " at index " + currentI + " (expected " + (char) expectedData[currentI] + " "
                                 + "found " + (char) b + ")");
                }
            }

            if (reportedBadBoy.get()) {
                System.out.print((char) b);
                System.out.flush();
            }
        }
        data.resetReaderIndex();
    }

    private void reportBadBoy(String x) {
        if (reportedBadBoy.compareAndSet(false, true)) {
            System.out.println(x);
        }
    }
}
