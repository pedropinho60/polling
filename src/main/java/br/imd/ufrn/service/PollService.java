package br.imd.ufrn.service;

import br.imd.ufrn.grpc.GrpcPollService;
import br.imd.ufrn.heartbeat.HeartbeatManager;
import br.imd.ufrn.http.HttpUtil;
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

    private final String gatewayAddress;
    private final int gatewayHbPort;

    private final int serviceHbPort;

    private final HeartbeatManager hb;

    public PollService(int udpPort, int httpPort, int grpcPort, int serviceHbPort, String gatewayAddress, int gatewayHbPort) {
        serviceUdpPort = udpPort;
        serviceHttpPort = httpPort;
        serviceGrpcPort = grpcPort;
        this.gatewayAddress = gatewayAddress;
        this.gatewayHbPort = gatewayHbPort;
        this.serviceHbPort = serviceHbPort;

        hb = new HeartbeatManager(serviceHbPort);
    }

    public void startHeartbeatListener() {
        System.out.println("Listening for heatbeat on port " + serviceHbPort);
        hb.listen();
    }


    public void startHeartbeat() {
        try (DatagramSocket hbSocket = new DatagramSocket()) {
            InetAddress gatewayAddress = InetAddress.getByName(this.gatewayAddress);
            String portMsg = serviceUdpPort + "," + serviceHttpPort + "," + serviceGrpcPort;
            byte[] data = portMsg.getBytes();

            while (true) {
                DatagramPacket packet = new DatagramPacket(data, data.length, gatewayAddress, gatewayHbPort);
                hbSocket.send(packet);
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runUdp() {
        try (DatagramSocket serverSocket = new DatagramSocket(serviceUdpPort)){
            System.out.println("UDP server started on port " + serviceUdpPort);
            while (true) {
                byte[] receiveMessage = new byte[1024];
                DatagramPacket receivePacket = new DatagramPacket(receiveMessage, receiveMessage.length);
                serverSocket.receive(receivePacket);

                String message = new String(receivePacket.getData(), 0, receivePacket.getLength());

                InetAddress clientAddress = receivePacket.getAddress();
                int clientPort = receivePacket.getPort();

                Thread.startVirtualThread(() -> {
                    handleUdpClient(message, clientAddress, clientPort, serverSocket);
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("UDP server terminating");
        }
    }

    public void handleUdpClient(String message, InetAddress clientAddress, int clientPort, DatagramSocket serverSocket) {
        InetSocketAddress dbAddress = hb.getNextAvailableUdpService();

        if (dbAddress == null) {
            String error = "Error: No database available\n";
            sendUdpResponse(error, clientAddress, clientPort, serverSocket);

            return;
        }

        try (DatagramSocket serviceSocket = new DatagramSocket()) {
            DatagramPacket dbPacket = new DatagramPacket(message.getBytes(), message.length(), dbAddress.getAddress(), dbAddress.getPort());

            serviceSocket.send(dbPacket);

            serviceSocket.setSoTimeout(3000);

            byte[] responseMessage = new byte[1024];
            DatagramPacket responsePacket = new DatagramPacket(responseMessage, responseMessage.length);

            try {
                serviceSocket.receive(responsePacket);

                String response = new String(responsePacket.getData(), 0, responsePacket.getLength());
                sendUdpResponse(response, clientAddress, clientPort, serverSocket);
            } catch (IOException e) {
                sendUdpResponse("Error: Database timed out.\n", clientAddress, clientPort, serverSocket);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendUdpResponse(String message, InetAddress address, int port, DatagramSocket socket) {
        try {
            DatagramPacket packet = new DatagramPacket(message.getBytes(), message.getBytes().length, address, port);

            socket.send(packet);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void runHttp() {
        try (ServerSocket serverSocket = new ServerSocket(serviceHttpPort, 300)) {
            System.out.println("HTTP server started on port " + serviceHttpPort);

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
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("HTTP server terminating");
        }
    }

    private void handleHttpClient(Socket clientSocket) throws IOException {
        InetSocketAddress dbAddress = hb.getNextAvailableHttpService();

        if (dbAddress == null) {
            HttpUtil.sendHttpResponse(clientSocket, 500, "Error: No database available");
            return;
        }

        try (Socket serviceSocket = new Socket(dbAddress.getAddress(), dbAddress.getPort())) {
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
                    .forPort(serviceGrpcPort)
                    .addService(new GrpcPollService(hb))
                    .build()
                    .start();

            System.out.println("gRPC server started on port " + serviceGrpcPort);

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
        int serviceHbPort = Integer.parseInt(System.getenv().getOrDefault("SERVICE_HB_PORT", "9000"));
        String gatewayAddress = System.getenv().getOrDefault("GATEWAY_ADDR", "127.0.0.1");
        int gatewayHbPort = Integer.parseInt(System.getenv().getOrDefault("GATEWAY_HB_PORT", "9000"));

        PollService service = new PollService(udpPort, httpPort, grpcPort, serviceHbPort, gatewayAddress, gatewayHbPort);

        Thread heartbeat = Thread.startVirtualThread(service::startHeartbeat);
        Thread heartbeatListener = Thread.startVirtualThread(service::startHeartbeatListener);
        Thread udp = Thread.startVirtualThread(service::runUdp);
        Thread http = Thread.startVirtualThread(service::runHttp);
        Thread grpc = Thread.startVirtualThread(service::runGrpc);

        heartbeat.join();
        heartbeatListener.join();
        udp.join();
        http.join();
        grpc.join();
    }
}
