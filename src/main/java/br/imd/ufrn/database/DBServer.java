package br.imd.ufrn.database;

import br.imd.ufrn.grpc.GrpcDB;
import br.imd.ufrn.http.CreatePoll;
import br.imd.ufrn.http.VotePoll;
import br.imd.ufrn.model.*;
import br.imd.ufrn.udp.CreateOperation;
import br.imd.ufrn.udp.GetOperation;
import br.imd.ufrn.udp.Operation;
import br.imd.ufrn.udp.VoteOperation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.*;
import java.net.*;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import static br.imd.ufrn.http.HttpUtil.getHttpBody;
import static br.imd.ufrn.http.HttpUtil.sendHttpResponse;

public class DBServer {
    PollDatabase db = new PollDatabase();

    public void runUdp() {
        try (DatagramSocket serverSocket = new DatagramSocket(9092)){
            while (true) {
                byte[] receiveMessage = new byte[1024];
                DatagramPacket receivePacket = new DatagramPacket(receiveMessage, receiveMessage.length);
                serverSocket.receive(receivePacket);

                String message = new String(receivePacket.getData(), 0, receivePacket.getLength());

                ObjectMapper mapper = new ObjectMapper();

                String response;

                try {
                    Operation operation = mapper.readValue(message, Operation.class);
                    switch (operation) {
                        case CreateOperation createOperation -> {
                            Poll poll = new Poll(createOperation.name(), createOperation.options());
                            db.createPoll(poll);
                            response = "Created poll `" + poll.getName() + "`\n";
                        }
                        case GetOperation getOperation -> {
                            Poll poll = db.getPoll(getOperation.poll());

                            String optionsStr = poll.getOptions().entrySet().stream()
                                    .map(entry -> "`" + entry.getKey() + "`: " + entry.getValue() + " votes")
                                    .collect(Collectors.joining(", "));

                            response = "Get poll: name=" +
                                    poll.getName() +
                                    ", options=[" +
                                    optionsStr +
                                    "]\n";
                        }
                        case VoteOperation voteOperation -> {
                            db.vote(voteOperation.poll(), voteOperation.option());
                            response = "Voted for option `" +
                                    voteOperation.option() +
                                    "` on poll `" +
                                    voteOperation.poll() +
                                    "`\n";
                        }
                    }
                } catch (JsonProcessingException e) {
                    response = "Error while processing json\n";
                } catch (RuntimeException e) {
                    response = "Error: " + e.getMessage() + "\n";
                }

                InetAddress gatewayAddress = receivePacket.getAddress();
                int gatewayPort = receivePacket.getPort();

                System.out.print(response);

                DatagramPacket sendResponsePacket = new DatagramPacket(response.getBytes(), response.getBytes().length, gatewayAddress, gatewayPort);

                serverSocket.send(sendResponsePacket);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("UDP Server Terminating");
        }
    }

    public void runHttp() {
        try (ServerSocket serverSocket = new ServerSocket(8082, 300)) {
            System.out.println("HTTP Server Started on port " + 8082);

            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {

                    String headerLine = in.readLine();
                    StringTokenizer tokenizer = new StringTokenizer(headerLine);
                    String httpMethod = tokenizer.nextToken();

                    if (httpMethod.equals("GET")) {
                        String httpQueryString = tokenizer.nextToken();
                        String[] parts = httpQueryString.split("/");

                        if (parts.length > 2 && parts[1].equals("polls")) {
                            try {
                                Poll poll = db.getPoll(parts[2]);

                                String optionsStr = poll.getOptions().entrySet().stream()
                                        .map(entry -> "`" + entry.getKey() + "`: " + entry.getValue() + " votes")
                                        .collect(Collectors.joining(", "));

                                String response = "Get poll: name=" +
                                        poll.getName() +
                                        ", options=[" +
                                        optionsStr +
                                        "]\n";

                                sendHttpResponse(clientSocket, 200, response);
                            } catch (Exception e) {
                                sendHttpResponse(clientSocket, 404, "Poll `" + parts[2] + "` not found");
                            }
                        } else {
                            sendHttpResponse(clientSocket, 404, "Not found");
                        }
                    } else if (httpMethod.equals("POST")) {
                        String httpQueryString = tokenizer.nextToken();
                        String[] parts = httpQueryString.split("/");

                        try {
                            if (httpQueryString.equals("/polls") || httpQueryString.equals("/polls/")) {
                                String json = getHttpBody(in);

                                ObjectMapper mapper = new ObjectMapper();
                                CreatePoll poll = mapper.readValue(json, CreatePoll.class);

                                db.createPoll(new Poll(poll.name(), poll.options()));

                                String response = "Created poll `" + poll.name() + "`\n";
                                sendHttpResponse(clientSocket, 200, response);
                            } else if (parts.length > 3 && parts[1].equals("polls") && parts[3].equals("vote")) {
                                String pollName = parts[2];

                                String json = getHttpBody(in);
                                ObjectMapper mapper = new ObjectMapper();
                                VotePoll poll = mapper.readValue(json, VotePoll.class);

                                db.vote(pollName, poll.option());

                                String response = "Voted for option `" +
                                        poll.option() +
                                        "` on poll `" +
                                        pollName +
                                        "`\n";

                                sendHttpResponse(clientSocket, 200, response);
                            } else {
                                sendHttpResponse(clientSocket, 404, "Not found");
                            }
                        } catch (Exception e) {
                            sendHttpResponse(clientSocket, 400, "Bad request");
                        }
                    } else {
                        sendHttpResponse(clientSocket, 405, "Method not allowed");
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("HTTP Server Terminating");
        }
    }

    public void runGrpc() {
        try {
            Server server = ServerBuilder
                    .forPort(50053)
                    .addService(new GrpcDB(db))
                    .build()
                    .start();

            System.out.println("gRPC server started on port " + 50053);

            server.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("gRPC Server Terminating");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        DBServer server = new DBServer();
        Thread udp = Thread.startVirtualThread(server::runUdp);
        Thread http = Thread.startVirtualThread(server::runHttp);
        Thread grpc = Thread.startVirtualThread(server::runGrpc);

        udp.join();
        http.join();
        grpc.join();
    }
}
