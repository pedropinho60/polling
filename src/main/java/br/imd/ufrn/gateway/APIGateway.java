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
    final int gatewayHttpPort;
    final int gatewayUdpPort;
    final int gatewayGrpcPort;
    final int gatewayHbPort;

    final HeartbeatManager hb;

    public APIGateway(int httpPort, int udpPort, int grpcPort, int gatewayHbPort) {
        gatewayHttpPort = httpPort;
        gatewayUdpPort = udpPort;
        gatewayGrpcPort = grpcPort;
        this.gatewayHbPort = gatewayHbPort;
        hb = new HeartbeatManager(gatewayHbPort);
    }

    public void startHeartbeatListener() {
        System.out.println("Listening for heatbeat on port " + gatewayHbPort);
        hb.listen();
    }

    public void runUdp() {
        try (DatagramSocket serverSocket = new DatagramSocket(gatewayUdpPort)){
            System.out.println("UDP server started on port " + gatewayUdpPort);
            while (true) {
                byte[] receiveMessage = new byte[1024];
                DatagramPacket receivePacket = new DatagramPacket(receiveMessage, receiveMessage.length);

                serverSocket.receive(receivePacket);

                String message = new String(receivePacket.getData(), 0, receivePacket.getLength());

                InetAddress clientAddress = receivePacket.getAddress();
                int clientPort = receivePacket.getPort();

                Thread.startVirtualThread(() -> {
                    handleUdpClient(message, clientAddress, clientPort);
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("UDP server terminating");
        }
    }

    public void handleUdpClient(String message, InetAddress clientAddress, int clientPort) {
        InetSocketAddress serviceAddress = hb.getNextAvailableUdpService();

        if (serviceAddress == null) {
            String error = "Error: No Poll services available\n";
            sendUdpResponse(error, clientAddress, clientPort);

            return;
        }

        try (DatagramSocket serviceSocket = new DatagramSocket()) {
            DatagramPacket servicePacket = new DatagramPacket(message.getBytes(), message.getBytes().length, serviceAddress.getAddress(), serviceAddress.getPort());

            serviceSocket.send(servicePacket);

            serviceSocket.setSoTimeout(3000);

            byte[] responseMessage = new byte[1024];

            DatagramPacket responsePacket = new DatagramPacket(responseMessage, responseMessage.length);

            try {
                serviceSocket.receive(responsePacket);

                String response = new String(responsePacket.getData(), 0, responsePacket.getLength());

                sendUdpResponse(response, clientAddress, clientPort);
            } catch (IOException e) {
                sendUdpResponse("Error: Service timed out.\n", clientAddress, clientPort);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendUdpResponse(String message, InetAddress address, int port) {
        try (DatagramSocket socket = new DatagramSocket()) {
            DatagramPacket packet = new DatagramPacket(message.getBytes(), message.getBytes().length, address, port);

            socket.send(packet);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void runHttp() {
        try (ServerSocket serverSocket = new ServerSocket(gatewayHttpPort, 300)) {
            System.out.println("HTTP server Started on port " + gatewayHttpPort);

            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {
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
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("HTTP server terminating");
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
            System.out.println("gRPC server terminating");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int httpPort = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));
        int udpPort = Integer.parseInt(System.getenv().getOrDefault("UDP_PORT", "9090"));
        int grpcPort = Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT", "50051"));
        int gatewayHbPort = Integer.parseInt(System.getenv().getOrDefault("GATEWAY_HB_PORT", "9000"));

        APIGateway gateway = new APIGateway(httpPort, udpPort, grpcPort, gatewayHbPort);

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
