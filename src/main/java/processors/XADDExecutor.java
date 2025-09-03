package processors;

import db.DataStore;
import models.DataStoreValue;
import models.RespCommand;
import utility.RespUtility;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class XADDExecutor implements CommandExecutor {

  @Override
  public String execute(RespCommand cmd) {
    String key = cmd.getKey();
    // Later remove sublist when RespCommand is updated
    List<String> args = cmd.getArgs().subList(1, cmd.getArgsSize());

    String entryID = args.get(0);
    Map<String, String> entryValue = new HashMap<>();
    for (int i = 1; i < args.size(); i += 2) {
      entryValue.put(args.get(i), args.get(i + 1));
    }

    DataStoreValue existingData = DataStore.get(key);
    ConcurrentNavigableMap<String, Map<String, String>> stream;
    if (existingData == null) {
      stream = new ConcurrentSkipListMap<>();
      DataStore.put(key, new DataStoreValue(stream));
    } else {
      try {
        stream = existingData.getAsStream();
      } catch (Exception e) {
        return RespUtility.buildErrorResponse(
            "WRONGTYPE Operation against a key holding the wrong kind of value");
      }
    }

    String validationError = validateStreamID(entryID, stream);
    if (validationError != null) {
      return RespUtility.buildErrorResponse(validationError);
    }

    stream.put(entryID, entryValue);
    return RespUtility.serializeResponse(entryID);
  }

  /**
   * Validates stream ID according to Redis rules:
   * 1. Must match <ms>-<seq> format
   * 2. Must be strictly greater than last ID in stream
   * 3. Must be >= 0-1
   */
  private String validateStreamID(String entryID, ConcurrentNavigableMap<String, Map<String, String>> stream) {
    if (!entryID.matches("\\d+-\\d+")) {
      return "invalid stream ID format";
    }

    String[] parts = entryID.split("-");
    long ms = Long.parseLong(parts[0]);
    long seq = Long.parseLong(parts[1]);

    if (ms == 0 && seq == 0) {
      return "The ID specified in XADD must be greater than 0-0";
    }

    if (!stream.isEmpty()) {
      String lastId = stream.lastKey();
      if (!isGreater(entryID, lastId)) {
        return "The ID specified in XADD is equal or smaller than the target stream top item";
      }
    }

    return null; // Valid
  }

  /**
   * Compares two IDs of form ms-seq.
   */
  private boolean isGreater(String newId, String oldId) {
    String[] newParts = newId.split("-");
    String[] oldParts = oldId.split("-");
    long newMs = Long.parseLong(newParts[0]);
    long newSeq = Long.parseLong(newParts[1]);
    long oldMs = Long.parseLong(oldParts[0]);
    long oldSeq = Long.parseLong(oldParts[1]);

    return (newMs > oldMs) || (newMs == oldMs && newSeq > oldSeq);
  }
}
