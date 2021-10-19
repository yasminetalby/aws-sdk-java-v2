package software.amazon.awssdk.http.nio.netty;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class BadBoyCatchingPublisher implements Publisher<ByteBuffer> {
    private static byte[] expectedData;
    private final Publisher<ByteBuffer> delegate;
    private final String name;

    public BadBoyCatchingPublisher(Publisher<ByteBuffer> delegate, String name) {
        this.delegate = delegate;
        this.name = name;
    }

    public static void initialize(byte[] expectedData) {
        BadBoyCatchingPublisher.expectedData = expectedData;
        ByteBufOrderVerifier.initialize(expectedData);
        ByteBufferOrderVerifier.initialize(expectedData);
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        delegate.subscribe(new BadBoyCatchingSubscriber(subscriber, name));
    }

    public static class BadBoyCatchingSubscriber implements Subscriber<ByteBuffer> {
        private final Subscriber<? super ByteBuffer> delegate;
        private final String name;
        private final AtomicInteger i = new AtomicInteger(0);
        private final AtomicBoolean reportedBadBoy = new AtomicBoolean(false);

        public BadBoyCatchingSubscriber(Subscriber<? super ByteBuffer> delegate, String name) {
            this.delegate = delegate;
            this.name = name;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            delegate.onSubscribe(subscription);
        }

        @Override
        public void onNext(ByteBuffer o) {
            if (expectedData != null && o.hasRemaining()) {
                o.mark();
                while (o.hasRemaining() && i.get() < expectedData.length) {
                    byte b = o.get();
                    int currentI = i.getAndIncrement();
                    if (expectedData[currentI] != b) {
                        reportBadBoy("Bad Boy: " + name + " at index " + i + "(expected " + b + " found " + expectedData[currentI] + ")");
                    }
                }
                o.reset();
            }

            delegate.onNext(o);
        }

        private void reportBadBoy(String x) {
            if (reportedBadBoy.compareAndSet(false, true)) {
                System.out.println(x);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            delegate.onComplete();
        }
    }
}
