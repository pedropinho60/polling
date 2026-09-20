package br.imd.ufrn.http;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class HttpUtil {
    public static String getHttpBody(BufferedReader in) throws IOException {
        String line;
        int contentLength = 0;

        while ((line = in.readLine()) != null && !line.isEmpty()) {
            if (line.toLowerCase().startsWith("content-length:")) {
                contentLength = Integer.parseInt(
                        line.substring("content-length:".length()).trim()
                );
            }
        }

        if (line == null) {
            throw new IOException("Unexpected end of headers");
        }

        char[] bodyChars = new char[contentLength];
        int offset = 0;

        while (offset < contentLength) {
            int read = in.read(bodyChars, offset, contentLength - offset);

            if (read == -1) {
                throw new IOException("Unexpected end of request body");
            }

            offset += read;
        }

        return new String(bodyChars);
    }

    public static void sendHttpResponse(Socket socket, int statusCode, String responseString) {
        String statusLine;
        String serverHeader = "Server: Polling\r\n";
        String contentTypeHeader = "Content-Type: text/plain; charset=UTF-8\r\n";
        byte[] body = responseString.getBytes(StandardCharsets.UTF_8);
        try (DataOutputStream out = new DataOutputStream(socket.getOutputStream());) {
            if (statusCode == 200) {
                statusLine = "HTTP/1.0 200 OK\r\n";
                String contentLengthHeader = "Content-Length: " + body.length + "\r\n";
                out.writeBytes(statusLine);
                out.writeBytes(serverHeader);
                out.writeBytes(contentTypeHeader);
                out.writeBytes(contentLengthHeader);
                out.writeBytes("\r\n");
                out.write(body);
            } else if (statusCode == 405) {
                statusLine = "HTTP/1.0 405 Method Not Allowed\r\n";
                out.writeBytes(statusLine);
                String contentLengthHeader = "Content-Length: 0\r\n";
                out.writeBytes(contentLengthHeader);
                out.writeBytes("\r\n");
            } else if (statusCode == 400) {
                statusLine = "HTTP/1.0 400 Bad Request\r\n";
                out.writeBytes(statusLine);
                String contentLengthHeader = "Content-Length: 0\r\n";
                out.writeBytes(contentLengthHeader);
                out.writeBytes("\r\n");
            } else if (statusCode == 500) {
                statusLine = "HTTP/1.0 500 Internal Server Error\r\n";
                out.writeBytes(statusLine);
                String contentLengthHeader = "Content-Length: 0\r\n";
                out.writeBytes(contentLengthHeader);
                out.writeBytes("\r\n");
            } else {
                statusLine = "HTTP/1.0 404 Not Found" + "\r\n";
                out.writeBytes(statusLine);
                String contentLengthHeader = "Content-Length: 0\r\n";
                out.writeBytes(contentLengthHeader);
                out.writeBytes("\r\n");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static byte[] readHttpHeaders(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        int previous = -1;
        int current;

        while ((current = in.read()) != -1) {
            out.write(current);

            if (previous == '\r' && current == '\n') {
                byte[] data = out.toByteArray();
                int len = data.length;

                if (len >= 4 &&
                        data[len - 4] == '\r' &&
                        data[len - 3] == '\n' &&
                        data[len - 2] == '\r' &&
                        data[len - 1] == '\n') {
                    return data;
                }
            }

            previous = current;
        }

        throw new IOException("Connection closed before HTTP headers completed");
    }

    public static long getContentLength(byte[] headers) {
        String headerString = new String(headers);

        for (String line : headerString.split("\r\n")) {
            if (line.regionMatches(true, 0, "Content-Length:", 0, "Content-Length:".length())) {
                return Long.parseLong(line.substring("Content-Length:".length()).trim());
            }
        }

        return 0;
    }

    public static void copyExactly(InputStream in, OutputStream out, long length) throws IOException {
        byte[] buffer = new byte[8192];

        while (length > 0) {
            int bytesToRead = (int) Math.min(buffer.length, length);

            int read = in.read(buffer, 0, bytesToRead);

            if (read == -1) {
                throw new IOException("Unexpected end of HTTP body");
            }

            out.write(buffer, 0, read);
            length -= read;
        }
    }

}
