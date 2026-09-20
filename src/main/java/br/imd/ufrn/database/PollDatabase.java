package br.imd.ufrn.database;

import br.imd.ufrn.PollResponse;
import br.imd.ufrn.model.Poll;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class PollDatabase {
    ConcurrentMap<String, Poll> polls = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, List<StreamObserver<PollResponse>>> subscribers = new ConcurrentHashMap<>();

    public void subscribe(String pollName, StreamObserver<PollResponse> observer) {
        if (!polls.containsKey(pollName)) {
            throw new RuntimeException("Poll `" + pollName + "` does not exist");
        }

        subscribers.computeIfAbsent(pollName, k -> new CopyOnWriteArrayList<>())
                .add(observer);
    }

    public void unsubscribe(String pollName, StreamObserver<PollResponse> observer) {
        var observers = subscribers.get(pollName);

        if (observers != null) {
            observers.remove(observer);
        }
    }

    public void createPoll(Poll poll) {
        if (polls.containsKey(poll.getName())) {
            throw new RuntimeException("Poll `" + poll.getName() +"` already exists");
        }

        polls.put(poll.getName(), poll);
    }

    public Poll getPoll(String pollName) {
        if (!polls.containsKey(pollName)) {
            throw new RuntimeException("Poll `" + pollName +"` does not exist");
        }

        return polls.get(pollName);
    }

    public void vote(String pollName, String option) {
        if (!polls.containsKey(pollName)) {
            throw new RuntimeException("Poll `" + pollName +"` does not exist");
        }

        Poll poll = polls.get(pollName);

        if (!poll.getOptions().containsKey(option)) {
            throw new RuntimeException("Poll `" + pollName + "` does not have option `" + option + "`");
        }

        poll.getOptions().compute(option, (k, v) -> (v == null) ? 1 : v + 1);

        var observers = subscribers.get(pollName);

        if (observers == null) {
            return;
        }

        var response = PollResponse.newBuilder().setName(pollName).putAllOptions(poll.getOptions()).build();

        for (var observer : observers) {
            observer.onNext(response);
        }
    }
}
