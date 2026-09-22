package br.imd.ufrn.database;

import br.imd.ufrn.grpc.GrpcDB;
import br.imd.ufrn.http.CreatePoll;
import br.imd.ufrn.http.HttpUtil;
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

public class DBServer implements Closeable{
    PollDatabase db = new PollDatabase();

    private final int httpPort;
    private final int udpPort;
    private final int grpcPort;

    private final String[] serviceAddresses;
    private final int serviceHbPort;

    private final ObjectMapper mapper = new ObjectMapper();


    public DBServer(int httpPort, int udpPort, int grpcPort, String[] serviceAddresses, int serviceHbPort) throws IOException {
        this.httpPort = httpPort;
        this.udpPort = udpPort;
        this.grpcPort = grpcPort;
        this.serviceAddresses = serviceAddresses;
        this.serviceHbPort = serviceHbPort;
    }

    public void startHeartbeat() {
        try (DatagramSocket hbSocket = new DatagramSocket()) {
            String portMsg = udpPort + "," + httpPort + "," + grpcPort;
            byte[] data = portMsg.getBytes();

            while (true) {
                for (String service : serviceAddresses) {
                    InetAddress gatewayAddress = InetAddress.getByName(service);
                    DatagramPacket packet = new DatagramPacket(data, data.length, gatewayAddress, serviceHbPort);
                    hbSocket.send(packet);
                }
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runUdp() {
        try (DatagramSocket serverSocket = new DatagramSocket(udpPort)){
            System.out.println("UDP server started on port " + udpPort);
            while (true) {
                try {
                    byte[] receiveMessage = new byte[1024];
                    DatagramPacket receivePacket = new DatagramPacket(receiveMessage, receiveMessage.length);
                    serverSocket.receive(receivePacket);

                    String message = new String(receivePacket.getData(), 0, receivePacket.getLength());

                    Thread.startVirtualThread(() -> {
                        try {
                            handleUdpClient(message, receivePacket.getAddress(), receivePacket.getPort(), serverSocket);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("UDP server terminating");
        }
    }

    public void handleUdpClient(String message, InetAddress clientAddress, int clientPort, DatagramSocket serverSocket) throws IOException {
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

        DatagramPacket sendResponsePacket = new DatagramPacket(response.getBytes(), response.getBytes().length, clientAddress, clientPort);

        serverSocket.send(sendResponsePacket);
    }

    public void runHttp() {
        try (ServerSocket serverSocket = new ServerSocket(httpPort, 300)) {
            System.out.println("HTTP server started on port " + httpPort);

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();

                    Thread.startVirtualThread(() -> {
                        try (clientSocket) {
                            handleHttpClient(clientSocket);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("HTTP server terminating");
        }
    }

    private void handleHttpClient(Socket clientSocket) throws IOException {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
            String headerLine = in.readLine();
            StringTokenizer tokenizer = new StringTokenizer(headerLine);
            String httpMethod = tokenizer.nextToken();

            if(httpMethod.equals("GET")) {
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
            } else if(httpMethod.equals("POST")) {
                String httpQueryString = tokenizer.nextToken();
                String[] parts = httpQueryString.split("/");

                try {
                    if (httpQueryString.equals("/polls") || httpQueryString.equals("/polls/")) {
                        String json = getHttpBody(in);

                        CreatePoll poll = mapper.readValue(json, CreatePoll.class);

                        db.createPoll(new Poll(poll.name(), poll.options()));

                        String response = "Created poll `" + poll.name() + "`\n";
                        sendHttpResponse(clientSocket, 200, response);
                    } else if (parts.length > 3 && parts[1].equals("polls") && parts[3].equals("vote")) {
                        String pollName = parts[2];

                        String json = getHttpBody(in);
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

    public void runGrpc() {
        try {
            Server server = ServerBuilder
                    .forPort(grpcPort)
                    .addService(new GrpcDB(db))
                    .build()
                    .start();

            System.out.println("gRPC server started on port " + grpcPort);

            server.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("gRPC server terminating");
        }
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    public static void main(String[] args) throws InterruptedException {
        int httpPort = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));
        int udpPort = Integer.parseInt(System.getenv().getOrDefault("UDP_PORT", "9090"));
        int grpcPort = Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT", "50051"));

        String serviceAddresses = System.getenv().getOrDefault("SERVICE_ADDRESSES", "127.0.0.1");
        int hbPort = Integer.parseInt(System.getenv().getOrDefault("SERVICE_HB_PORT", "9000"));

        String[] addresses = serviceAddresses.split(",");

        try (DBServer server = new DBServer(httpPort, udpPort, grpcPort, addresses, hbPort)) {
            Thread udp = Thread.startVirtualThread(server::runUdp);
            Thread http = Thread.startVirtualThread(server::runHttp);
            Thread grpc = Thread.startVirtualThread(server::runGrpc);
            Thread hb = Thread.startVirtualThread(server::startHeartbeat);

            udp.join();
            http.join();
            grpc.join();
            hb.join();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
