package br.imd.ufrn.gateway;

import br.imd.ufrn.grpc.GrpcPollService;
import br.imd.ufrn.heartbeat.HeartbeatManager;
import br.imd.ufrn.http.HttpUtil;
import io.grpc.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;

public class APIGateway {
    final int gatewayHttpPort = 8080;
    final int gatewayUdpPort = 9090;
    final int gatewayGrpcPort = 50051;

    HeartbeatManager hb = new HeartbeatManager();

    public void startHeartbeatListener() {
        hb.listen();
    }

    public void runUdp() {
        try (DatagramSocket serverSocket = new DatagramSocket(gatewayUdpPort)){
            while (true) {
                byte[] receiveMessage = new byte[1024];
                DatagramPacket receivePacket = new DatagramPacket(receiveMessage, receiveMessage.length);

                serverSocket.setSoTimeout(0);
                serverSocket.receive(receivePacket);
                String message = new String(receivePacket.getData(), 0, receivePacket.getLength());

                InetSocketAddress serviceAddress = hb.getNextAvailableUdpService();
                if (serviceAddress == null) {
                    String error = "Error: No Poll services available\n";
                    serverSocket.send(new DatagramPacket(error.getBytes(), error.getBytes().length,
                                                    receivePacket.getAddress(), receivePacket.getPort()));

                    continue;
                }
                System.out.println("service port: " + serviceAddress.getPort());

                DatagramPacket servicePacket = new DatagramPacket(message.getBytes(), message.getBytes().length,
                                                            serviceAddress.getAddress(), serviceAddress.getPort());
                serverSocket.send(servicePacket);

                byte[] responseMessage = new byte[1024];
                DatagramPacket responsePacket = new DatagramPacket(responseMessage, responseMessage.length,
                                                        serviceAddress.getAddress(), serviceAddress.getPort());

                try {
                    serverSocket.setSoTimeout(3000);
                    serverSocket.receive(responsePacket);

                    String response = new String(responsePacket.getData(), 0, responsePacket.getLength());

                    DatagramPacket sendResponsePacket = new DatagramPacket(response.getBytes(), response.getBytes().length,
                                                                receivePacket.getAddress(), receivePacket.getPort());
                    serverSocket.send(sendResponsePacket);
                } catch (SocketTimeoutException e) {
                    String error = "Error: Service timed out.\n";
                    serverSocket.send(new DatagramPacket(error.getBytes(), error.getBytes().length,
                                                        receivePacket.getAddress(), receivePacket.getPort()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("UDP Server Terminating");
        }
    }

    public void runHttp() {
        try (ServerSocket serverSocket = new ServerSocket(gatewayHttpPort, 300)) {
            System.out.println("HTTP Server Started on port " + gatewayHttpPort);

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
        InetSocketAddress serviceAddress = hb.getNextAvailableHttpService();

        if (serviceAddress == null) {
            HttpUtil.sendHttpResponse(clientSocket, 500, "Error: No poll services available");
            return;
        }

        try (Socket serviceSocket = new Socket(serviceAddress.getAddress(), serviceAddress.getPort())) {
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
            Server server = ServerBuilder
                    .forPort(gatewayGrpcPort)
                    .addService(new GrpcPollService(hb))
                    .build()
                    .start();

            System.out.println("gRPC server started on port " + gatewayGrpcPort);

            server.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("gRPC Server Terminating");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        APIGateway gateway = new APIGateway();
        Thread heartbeat = Thread.startVirtualThread(gateway::startHeartbeatListener);
        Thread udp = Thread.startVirtualThread(gateway::runUdp);
        Thread http = Thread.startVirtualThread(gateway::runHttp);
        Thread grpc = Thread.startVirtualThread(gateway::runGrpc);

        heartbeat.join();
        udp.join();
        http.join();
        grpc.join();
    }
}
