package br.imd.ufrn.grpc;

import br.imd.ufrn.*;
import br.imd.ufrn.gateway.APIGateway;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class GrpcPollService extends PollServiceGrpc.PollServiceImplBase {
    private final APIGateway gateway;

    public GrpcPollService(APIGateway gateway) {
        this.gateway = gateway;
    }

    private PollServiceGrpc.PollServiceStub getStub() {
        GrpcService service = gateway.getNextAvailableGrpcService();

        if (service == null) {
            throw new RuntimeException("No poll services available");
        }

        return PollServiceGrpc.newStub(service.getChannel());
    }

    @Override
    public void createPoll(PollCreation request, StreamObserver<Empty> responseObserver) {
        try {
            var stub = getStub();
            stub.createPoll(request, responseObserver);
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getPoll(PollName request, StreamObserver<PollResponse> responseObserver) {
        try {
            var stub = getStub();
            stub.getPoll(request, responseObserver);
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void votePoll(Vote request, StreamObserver<Empty> responseObserver) {
        try {
            var stub = getStub();
            stub.votePoll(request, responseObserver);
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }
}