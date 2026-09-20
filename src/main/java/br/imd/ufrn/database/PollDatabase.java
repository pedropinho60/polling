package br.imd.ufrn.database;

import br.imd.ufrn.model.Poll;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PollDatabase {
    ConcurrentMap<String, Poll> polls = new ConcurrentHashMap<>();

    public PollDatabase() {

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
    }
}
