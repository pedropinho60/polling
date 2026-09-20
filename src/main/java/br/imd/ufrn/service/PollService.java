package br.imd.ufrn.service;

import br.imd.ufrn.PollServiceGrpc;
import br.imd.ufrn.grpc.ServiceGrpcPollService;
import br.imd.ufrn.http.HttpUtil;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;

public class PollService {
    private final int serviceUdpPort;
    private final int serviceHttpPort;
    private final int serviceGrpcPort;
    private final int gatewayHeartbeatPort = 9000;

    public PollService(int udpPort, int httpPort, int grpcPort) {
        serviceUdpPort = udpPort;
        serviceHttpPort = httpPort;
        serviceGrpcPort = grpcPort;
    }

    public void startHeartbeat() {
        try (DatagramSocket hbSocket = new DatagramSocket()) {
            InetAddress gatewayAddress = InetAddress.getByName("127.0.0.1");
            String portMsg = serviceUdpPort + "," + serviceHttpPort + "," + serviceGrpcPort;
            byte[] data = portMsg.getBytes();

            while (true) {
                DatagramPacket packet = new DatagramPacket(data, data.length, gatewayAddress, gatewayHeartbeatPort);
                hbSocket.send(packet);
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runUdp() {
        try (DatagramSocket serverSocket = new DatagramSocket(serviceUdpPort)){
            while (true) {
                byte[] receiveMessage = new byte[1024];
                DatagramPacket receivePacket = new DatagramPacket(receiveMessage, receiveMessage.length);
                serverSocket.receive(receivePacket);

                String message = new String(receivePacket.getData(), 0, receivePacket.getLength());

                InetAddress dbAddress = InetAddress.getByName("127.0.0.1");
                int dbPort = 9092;

                DatagramPacket dbPacket = new DatagramPacket(message.getBytes(), message.length(), dbAddress, dbPort);
                serverSocket.send(dbPacket);

                byte[] responseMessage = new byte[1024];
                DatagramPacket responsePacket = new DatagramPacket(responseMessage, responseMessage.length, dbAddress, dbPort);
                serverSocket.receive(responsePacket);

                String response = new String(responsePacket.getData(), 0, responsePacket.getLength());

                InetAddress gatewayAddress = receivePacket.getAddress();
                int gatewayPort = receivePacket.getPort();

                DatagramPacket sendResponsePacket = new DatagramPacket(response.getBytes(), response.getBytes().length, gatewayAddress, gatewayPort);
                serverSocket.send(sendResponsePacket);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("UDP Server Terminating");
        }
    }

    public void runHttp() {
        try (ServerSocket serverSocket = new ServerSocket(serviceHttpPort, 300)) {
            System.out.println("HTTP Server Started on port " + serviceHttpPort);

            while (true) {
                Socket clientSocket = serverSocket.accept();

                Thread.startVirtualThread(() -> {
                    try (clientSocket) {
                        handleHttpClient(clientSocket);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("HTTP Server Terminating");
        }
    }

    private void handleHttpClient(Socket clientSocket) throws IOException {
        InetAddress dbAddress = InetAddress.getByName("127.0.0.1");
        try (Socket serviceSocket = new Socket(dbAddress, 8082)) {
            InputStream clientIn = clientSocket.getInputStream();
            OutputStream clientOut = clientSocket.getOutputStream();

            InputStream serviceIn = serviceSocket.getInputStream();
            OutputStream serviceOut = serviceSocket.getOutputStream();

            byte[] requestHeaders = HttpUtil.readHttpHeaders(clientIn);
            serviceOut.write(requestHeaders);

            long requestBodyLength = HttpUtil.getContentLength(requestHeaders);

            HttpUtil.copyExactly(clientIn, serviceOut, requestBodyLength);

            byte[] responseHeaders = HttpUtil.readHttpHeaders(serviceIn);
            clientOut.write(responseHeaders);

            long responseBodyLength = HttpUtil.getContentLength(responseHeaders);

            HttpUtil.copyExactly(serviceIn, clientOut, responseBodyLength);

            clientOut.flush();
        }
    }

    public void runGrpc() {
        try {
            ManagedChannel dbChannel = ManagedChannelBuilder
                    .forAddress("127.0.0.1", 50053)
                    .usePlaintext()
                    .build();

            var db = PollServiceGrpc.newStub(dbChannel);

            Server server = ServerBuilder
                    .forPort(serviceGrpcPort)
                    .addService(new ServiceGrpcPollService(db))
                    .build()
                    .start();

            System.out.println("gRPC server started on port " + serviceGrpcPort);

            server.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("gRPC Server Terminating");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int udpPort = 9090;
        int httpPort = 8080;
        int grpcPort = 50052;
        if (args.length > 2) {
            try {
                udpPort = Integer.parseInt(args[0]);
                httpPort = Integer.parseInt(args[1]);
                grpcPort = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid ports provided, using defaults");
            }
        }

        PollService service = new PollService(udpPort, httpPort, grpcPort);

        Thread heartbeat = Thread.startVirtualThread(service::startHeartbeat);
        Thread udp = Thread.startVirtualThread(service::runUdp);
        Thread http = Thread.startVirtualThread(service::runHttp);
        Thread grpc = Thread.startVirtualThread(service::runGrpc);

        heartbeat.join();
        udp.join();
        http.join();
        grpc.join();
    }
}
