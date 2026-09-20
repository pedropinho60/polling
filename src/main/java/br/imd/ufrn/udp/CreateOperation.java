package br.imd.ufrn.udp;

public record CreateOperation(String name, String[] options) implements Operation {
}
