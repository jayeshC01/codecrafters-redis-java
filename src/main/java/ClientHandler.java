import java.lang.*;
import java.net.Socket;
import java.io.IOException;
import java.io.OutputStream;

class ClientHandler implements Runnable  {

  private final Socket clientSocket;

  public ClientHandler(Socket clientSocket) {
    this.clientSocket = clientSocket;
  }

  public void run() {
    try {
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(clientSocket.getInputStream()));
      BufferedWriter write =
          new BufferedWriter(
              new OutputStreamReader(clientSocket.getOutputStream()));
      String cmd;
      while ((cmd = reader.readLine()) != null) {
        // Read input from client.
        System.out.println("Received: " + cmd );
        String response = processCommand(cmd);
        writer.write(response);
        write.flush();
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

  public String processCommand(String cmd) {
    cmd = cmd.toLowerCase().trim();
    switch(cmd) {
      case "ping":
        return "PONG";
      case "echo": {
        String[] cmdparts = cmd.split(" ")
        if(cmdparts.length !=2) {
          return "-ERR invalid command ECHO"
        }
        return "$"+cmdparts[1].length+"\r\n"+cmdparts[1]+"\r\n";
      }
      default:
        return "Invalid Command"
    }
  }
}
