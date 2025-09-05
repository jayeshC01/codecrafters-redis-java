package processors.lists;

import db.DataStore;
import models.DataStoreValue;
import models.RespCommand;
import processors.CommandExecutor;
import utility.RespUtility;

import java.util.List;
import java.util.Queue;

public class BLPOPExecutor implements CommandExecutor {

  @Override
  public String execute(RespCommand cmd) {
    String key = cmd.getKey();
    double timeoutSeconds;
    long timeoutMillis;
    try {
      timeoutSeconds = Double.parseDouble(cmd.getArgs().get(1));
      timeoutMillis = (long) (timeoutSeconds * 1000);

    } catch (NumberFormatException e) {
      return RespUtility.buildErrorResponse("Invalid timeout argument");
    }

    DataStoreValue data = DataStore.get(key);
    // If data is present fetch and return it
    if (data != null && !data.getAsLinkedList().isEmpty()) {
      return RespUtility.serializeResponse(List.of(key, data.getAsLinkedList().poll()));
    }

    // Block the thread
    Thread currentThread = Thread.currentThread();
    DataStore.addWaiter(key, currentThread);
    boolean timedOut = false;
    synchronized (currentThread) {
      try {
        if (timeoutSeconds == 0) {
          currentThread.wait();  // wait indefinitely
        } else {
          currentThread.wait(timeoutMillis); // wait with timeout
          // After wait returns, check if we woke up due to timeout
          timedOut = true;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return RespUtility.buildErrorResponse("Interrupted while waiting");
      }
    }

    data = DataStore.get(key);
    // If data is present fetch and return it
    if (data != null && !data.getAsLinkedList().isEmpty()) {
      return RespUtility.serializeResponse(List.of(key, data.getAsLinkedList().poll()));
    }

    // If we timed out and no data arrived, remove from waiterThreads
    if (timedOut) {
      Queue<Thread> queue = DataStore.getWaiters(key);
      if (queue != null) {
        queue.remove(currentThread);
        if (queue.isEmpty()) {
          DataStore.cleanUpWaiter(key);
        }
      }
      return "*-1\r\n";
    }

    // Default fallback
    return RespUtility.serializeResponse(null);
  }
}
