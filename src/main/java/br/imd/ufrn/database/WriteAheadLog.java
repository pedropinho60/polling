package br.imd.ufrn.database;

import br.imd.ufrn.udp.Operation;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class WriteAheadLog implements Closeable {
    private final ObjectMapper objectMapper;
    private final BufferedWriter writer;

    public WriteAheadLog(ObjectMapper objectMapper, String filename) throws IOException {
        this.objectMapper = objectMapper;

        writer = Files.newBufferedWriter(Path.of(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized void append(Operation operation) throws IOException {
        writer.write(objectMapper.writeValueAsString(operation));
        writer.newLine();
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
