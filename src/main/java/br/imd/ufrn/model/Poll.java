package br.imd.ufrn.model;

import java.util.HashMap;
import java.util.Map;

public class Poll {
    String name;
    Map<String, Long> options = new HashMap<>();

    public Poll(String name, String[] options) {
        this.name = name;

        for (String key : options) {
            this.options.putIfAbsent(key, 0L);
        }
    }

    public String getName() {
        return this.name;
    }

    public Map<String, Long> getOptions() {
        return this.options;
    }
}