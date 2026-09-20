package br.imd.ufrn.heartbeat;

import br.imd.ufrn.grpc.GrpcService;
import jakarta.annotation.Nullable;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HeartbeatManager {
    ConcurrentMap<InetSocketAddress, Long> activeUdpServices = new ConcurrentHashMap<>();
    ConcurrentMap<InetSocketAddress, Long> activeHttpServices = new ConcurrentHashMap<>();
    ConcurrentMap<InetSocketAddress, GrpcService> activeGrpcServices = new ConcurrentHashMap<>();
    final int heartbeatPort = 9000;
    final long serviceTimeoutMs = 5000;
    int roundRobinIndex = 0;

    public void listen() {
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

    public synchronized InetSocketAddress getNextAvailableUdpService() {
        return getNextAvailable(activeUdpServices);
    }

    public synchronized InetSocketAddress getNextAvailableHttpService() {
        return getNextAvailable(activeHttpServices);
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
}
