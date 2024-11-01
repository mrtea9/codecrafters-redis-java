import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Main {
  public static void main(String[] args){

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        try {
          serverSocket = new ServerSocket(port);
          serverSocket.setReuseAddress(true);
          clientSocket = serverSocket.accept();
          OutputStream out = clientSocket.getOutputStream();
          InputStream in = clientSocket.getInputStream();
          BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
          String line;
          while ((line = reader.readLine()) != null) {
                System.out.println(line);
                if (line.trim().equalsIgnoreCase("PING")) {
                    out.write("+PONG\r\n".getBytes(StandardCharsets.UTF_8));
                    out.flush(); // Ensure the response is sent immediately
                }
          }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
          try {
            if (clientSocket != null) {
              clientSocket.close();
            }
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }
}
