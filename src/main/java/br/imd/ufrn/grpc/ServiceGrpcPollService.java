package br.imd.ufrn.grpc;

import br.imd.ufrn.*;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;

public class ServiceGrpcPollService extends PollServiceGrpc.PollServiceImplBase {
    private final PollServiceGrpc.PollServiceStub backend;

    public ServiceGrpcPollService(PollServiceGrpc.PollServiceStub service) {
        this.backend = service;
    }

    @Override
    public void createPoll(PollCreation request, StreamObserver<Empty> responseObserver) {
        backend.createPoll(request, responseObserver);
    }

    @Override
    public void getPoll(PollName request, StreamObserver<PollResponse> responseObserver) {
        backend.getPoll(request, responseObserver);
    }

    @Override
    public void votePoll(Vote request, StreamObserver<Empty> responseObserver) {
        backend.votePoll(request, responseObserver);
    }

    @Override
    public void notifyVotes(PollName request, StreamObserver<PollResponse> responseObserver) {
        backend.notifyVotes(request, responseObserver);
    }
}
