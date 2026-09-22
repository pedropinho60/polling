package br.imd.ufrn.grpc;

import br.imd.ufrn.*;
import br.imd.ufrn.heartbeat.HeartbeatManager;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.TimeUnit;

public class GrpcPollService extends PollServiceGrpc.PollServiceImplBase {
    private final HeartbeatManager hb;

    public GrpcPollService(HeartbeatManager hb) {
        this.hb = hb;
    }

    private PollServiceGrpc.PollServiceStub getStub() {
        GrpcService service = hb.getNextAvailableGrpcService();

        if (service == null) {
            throw Status.UNAVAILABLE.withDescription("No poll services available")
                    .asRuntimeException();
        }

        return PollServiceGrpc.newStub(service.getChannel());
    }

    @Override
    public void createPoll(PollCreation request, StreamObserver<Empty> responseObserver) {
        try {
            var stub = getStub().withDeadlineAfter(3, TimeUnit.SECONDS);
            stub.createPoll(request, responseObserver);
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getPoll(PollName request, StreamObserver<PollResponse> responseObserver) {
        try {
            var stub = getStub().withDeadlineAfter(3, TimeUnit.SECONDS);
            stub.getPoll(request, responseObserver);
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void votePoll(Vote request, StreamObserver<Empty> responseObserver) {
        try {
            var stub = getStub().withDeadlineAfter(3, TimeUnit.SECONDS);
            stub.votePoll(request, responseObserver);
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void notifyVotes(PollName request, StreamObserver<PollResponse> responseObserver) {
        try {
            var stub = getStub();
            stub.notifyVotes(request, responseObserver);
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }
}