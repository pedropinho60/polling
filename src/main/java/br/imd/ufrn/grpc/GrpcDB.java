package br.imd.ufrn.grpc;

import br.imd.ufrn.*;
import br.imd.ufrn.model.Poll;
import br.imd.ufrn.database.PollDatabase;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class GrpcDB extends PollServiceGrpc.PollServiceImplBase {
    private final PollDatabase db;

    public GrpcDB(PollDatabase db) {
        this.db = db;
    }

    @Override
    public void createPoll(PollCreation request, StreamObserver<Empty> responseObserver) {
        var options = request.getOptionsList();
        var poll = new Poll(request.getName(), options.toArray(new String[0]));
        try {
            db.createPoll(poll);
            Empty empty = Empty.newBuilder().build();
            responseObserver.onNext(empty);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getPoll(PollName request, StreamObserver<PollResponse> responseObserver) {
        try {
            Poll poll = db.getPoll(request.getName());

            var response = PollResponse.newBuilder()
                    .setName(poll.getName())
                    .putAllOptions(poll.getOptions())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void votePoll(Vote request, StreamObserver<Empty> responseObserver) {
        try {
            db.vote(request.getPollName(), request.getOption());

            Empty empty = Empty.newBuilder().build();

            responseObserver.onNext(empty);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
