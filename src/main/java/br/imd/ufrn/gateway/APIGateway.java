package br.imd.ufrn.gateway;

import br.imd.ufrn.grpc.GrpcPollService;
import br.imd.ufrn.grpc.GrpcService;
import br.imd.ufrn.http.HttpUtil;
import io.grpc.*;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class APIGateway {
    ConcurrentMap<InetSocketAddress, Long> activeUdpServices = new ConcurrentHashMap<>();
    ConcurrentMap<InetSocketAddress, Long> activeHttpServices = new ConcurrentHashMap<>();
    ConcurrentMap<InetSocketAddress, GrpcService> activeGrpcServices = new ConcurrentHashMap<>();

    final int gatewayHttpPort = 8080;
    final int gatewayUdpPort = 9090;
    final int gatewayGrpcPort = 50051;
    final int heartbeatPort = 9000;
    final long serviceTimeoutMs = 5000;
    int roundRobinIndex = 0;

    public void startHeartbeatListener() {
        try (DatagramSocket hbSocket = new DatagramSocket(heartbeatPort)) {
            while (true) {
                byte[] buf = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                hbSocket.receive(packet);

                String portStr = new String(packet.getData(), 0, packet.getLength()).trim();
                try {
                    String[] parts = portStr.split(",");

                    int serviceUdpPort = Integer.parseInt(parts[0]);
                    InetSocketAddress udpAddress = new InetSocketAddress(packet.getAddress(), serviceUdpPort);

                    activeUdpServices.put(udpAddress, System.currentTimeMillis());

                    int serviceHttpPort = Integer.parseInt(parts[1]);
                    InetSocketAddress httpAddress = new InetSocketAddress(packet.getAddress(), serviceHttpPort);

                    activeHttpServices.put(httpAddress, System.currentTimeMillis());

                    int serviceGrpcPort = Integer.parseInt(parts[2]);
                    InetSocketAddress grpcAddress = new InetSocketAddress(packet.getAddress(), serviceGrpcPort);

                    activeGrpcServices.compute(grpcAddress, (key, existing) -> {
                        if (existing == null) {
                            return new GrpcService(key);
                        }

                        existing.updateLastSeen();
                        return existing;
                    });
                } catch (Exception e) {
                    System.err.println("Wrong hearbeat received: " + portStr);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private synchronized InetSocketAddress getNextAvailableUdpService() {
        return getNextAvailable(activeUdpServices);
    }

    private synchronized InetSocketAddress getNextAvailableHttpService() {
        return getNextAvailable(activeHttpServices);
    }

    public synchronized GrpcService getNextAvailableGrpcService() {
        long now = System.currentTimeMillis();

        activeGrpcServices.entrySet().removeIf(entry -> {
            GrpcService service = entry.getValue();

            if (now - service.getLastSeen() > serviceTimeoutMs) {
                service.shutdown();
                return true;
            }

            return false;
        });

        if (activeGrpcServices.isEmpty()) {
            return null;
        }

        List<GrpcService> services = new ArrayList<>(activeGrpcServices.values());

        GrpcService selected = services.get(roundRobinIndex % services.size());

        roundRobinIndex++;

        return selected;
    }

    @Nullable
    private InetSocketAddress getNextAvailable(ConcurrentMap<InetSocketAddress, Long> activeServices) {
        long now = System.currentTimeMillis();
        activeServices.entrySet().removeIf(entry -> (now - entry.getValue()) > serviceTimeoutMs);

        if (activeServices.isEmpty()) return null;

        List<InetSocketAddress> services = new ArrayList<>(activeServices.keySet());
        InetSocketAddress selected = services.get(roundRobinIndex % services.size());
        roundRobinIndex++;

        return selected;
    }

    public void runUdp() {
        try (DatagramSocket serverSocket = new DatagramSocket(gatewayUdpPort)){
            while (true) {
                byte[] receiveMessage = new byte[1024];
                DatagramPacket receivePacket = new DatagramPacket(receiveMessage, receiveMessage.length);

                serverSocket.setSoTimeout(0);
                serverSocket.receive(receivePacket);
                String message = new String(receivePacket.getData(), 0, receivePacket.getLength());

                InetSocketAddress serviceAddress = getNextAvailableUdpService();
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
        InetSocketAddress serviceAddress = getNextAvailableHttpService();

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
                    .addService(new GrpcPollService(this))
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
