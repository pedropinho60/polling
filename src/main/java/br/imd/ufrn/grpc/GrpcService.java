package br.imd.ufrn.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.net.InetSocketAddress;

public class GrpcService {
    private final ManagedChannel channel;
    private volatile long lastSeen;

    public GrpcService(InetSocketAddress address) {
        this.channel = ManagedChannelBuilder
                .forAddress(address.getHostString(), address.getPort())
                .usePlaintext()
                .build();
        this.lastSeen = System.currentTimeMillis();
    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public void updateLastSeen() {
        lastSeen = System.currentTimeMillis();
    }

    public void shutdown() {
        channel.shutdown();
    }
}
