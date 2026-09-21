package br.imd.ufrn.database;

import br.imd.ufrn.PollResponse;
import br.imd.ufrn.model.Poll;
import br.imd.ufrn.udp.CreateOperation;
import br.imd.ufrn.udp.GetOperation;
import br.imd.ufrn.udp.Operation;
import br.imd.ufrn.udp.VoteOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.stub.StreamObserver;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class PollDatabase implements Closeable {
    private final ConcurrentMap<String, Poll> polls = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<StreamObserver<PollResponse>>> subscribers = new ConcurrentHashMap<>();
    private final WriteAheadLog wal;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private boolean initialized = false;

    public PollDatabase() throws IOException {
        replayWal();

        wal = new WriteAheadLog(objectMapper, "polls.wal");
        initialized = true;
    }

    public void createPoll(Poll poll) {
        if (polls.containsKey(poll.getName())) {
            throw new RuntimeException("Poll `" + poll.getName() +"` already exists");
        }

        if (initialized) {
            try {
                String name = poll.getName();
                String[] options = poll.getOptions().keySet().toArray(new String[0]);
                wal.append(new CreateOperation(name, options));
            } catch (IOException e) {
                throw new RuntimeException("Internal Server Error");
            }
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

        if (initialized) {
            try {
                wal.append(new VoteOperation(pollName, option));
            } catch (IOException e) {
                throw new RuntimeException("Internal Server Error");
            }
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

            if (observers.isEmpty()) {
                subscribers.remove(pollName, observers);
            }
        }
    }

    private void replayWal() throws IOException {
        Path path = Path.of("polls.wal");

        if (!Files.exists(path)) {
            return;
        }

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;

            while ((line = reader.readLine()) != null) {
                Operation operation = objectMapper.readValue(line, Operation.class);

                switch(operation) {
                    case CreateOperation createOperation -> {
                        Poll poll = new Poll(createOperation.name(), createOperation.options());
                        createPoll(poll);
                    }
                    case VoteOperation voteOperation -> {
                        vote(voteOperation.poll(), voteOperation.option());
                    }
                    case GetOperation ignored -> {
                    }
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        wal.close();
    }
}
