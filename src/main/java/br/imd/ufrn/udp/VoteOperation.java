package br.imd.ufrn.udp;

public record VoteOperation(String poll, String option) implements Operation {
}
