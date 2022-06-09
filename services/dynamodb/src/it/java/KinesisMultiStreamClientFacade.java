package software.amazon.awssdk.services.dynamodb;

import java.security.InvalidParameterException;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.NotImplementedException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamConsumersRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamConsumersResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;
import software.amazon.awssdk.services.kinesis.paginators.ListStreamConsumersPublisher;


/**
 * A wrapper for Kinesis client to allow for KCL 2.0 to consume multiple streams.
 * !! WARNING !!
 * This class prefixes all shardIds with a stream id, which makes KCL Table records incompatible.
 * You can't add or remove this wrapper on live application without ruining your KCL table.
 */
public class KinesisMultiStreamClientFacade implements KinesisAsyncClient {
    final KinesisAsyncClient wrappedClient;

    public KinesisMultiStreamClientFacade(KinesisAsyncClient wrappedClient) {
        if (wrappedClient instanceof KinesisMultiStreamClientFacade) {
            throw new InvalidParameterException("wrappedClient is an instance of KinesisMultiStreamClientFacade. Don't double-wrap the client.");
        }
        this.wrappedClient = wrappedClient;
    }

    @Override
    public String serviceName() {
        return wrappedClient.serviceName();
    }

    @Override
    public void close() {
        wrappedClient.close();
    }

    @Override
    public CompletableFuture<GetRecordsResponse> getRecords(GetRecordsRequest getRecordsRequest) {
        return wrappedClient.getRecords(getRecordsRequest);
    }

    @Override
    public CompletableFuture<GetShardIteratorResponse> getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        return wrappedClient.getShardIterator(getShardIteratorRequest);
    }

    @Override
    public CompletableFuture<ListShardsResponse> listShards(ListShardsRequest listShardsRequest) {

        /*
         * Async pagination across streams and shards is rather cumbersome and complex.
         * Current KCL implementation materializes a full list anyway, therefore the is no good reason to support
         * pagination in this client.
         *
         * This implementation loads entire list in a single invocation.
         * */

        return CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(100);
                return wrappedClient.listShards(listShardsRequest).join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<ListStreamConsumersResponse> listStreamConsumers(ListStreamConsumersRequest listStreamConsumersRequest) {
        throw new NotImplementedException("Not supported yet for multi-stream");
    }

    @Override
    public ListStreamConsumersPublisher listStreamConsumersPaginator(ListStreamConsumersRequest listStreamConsumersRequest) {
        throw new NotImplementedException("Not supported yet for multi-stream");
    }

    @Override
    public CompletableFuture<ListStreamsResponse> listStreams(ListStreamsRequest listStreamsRequest) {
        return wrappedClient.listStreams(listStreamsRequest);
    }

    @Override
    public CompletableFuture<ListStreamsResponse> listStreams() {
        return wrappedClient.listStreams();
    }

    /*
     * The following methods are implemented to support EFO for multi-stream.
     */
    @Override
    public CompletableFuture<DescribeStreamSummaryResponse> describeStreamSummary(
        DescribeStreamSummaryRequest describeStreamSummaryRequest) {
        return wrappedClient.describeStreamSummary(describeStreamSummaryRequest);
    }

    @Override
    public CompletableFuture<DescribeStreamConsumerResponse> describeStreamConsumer(DescribeStreamConsumerRequest describeStreamConsumerRequest) {
        return wrappedClient.describeStreamConsumer(describeStreamConsumerRequest);
    }

    @Override
    public CompletableFuture<RegisterStreamConsumerResponse> registerStreamConsumer(RegisterStreamConsumerRequest registerStreamConsumerRequest) {
        return wrappedClient.registerStreamConsumer(registerStreamConsumerRequest);
    }

    @Override
    public CompletableFuture<Void> subscribeToShard(SubscribeToShardRequest subscribeToShardRequest, SubscribeToShardResponseHandler asyncResponseHandler) {
        return wrappedClient.subscribeToShard(subscribeToShardRequest, asyncResponseHandler);
    }
}