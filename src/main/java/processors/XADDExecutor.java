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
    ConcurrentNavigableMap<String, Map<String ,String>> data = new ConcurrentSkipListMap<>();
    //Later remove the sublist part after RespCommand is updated to send arguments without key
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
        stream = existingData.getAsStream(); // casting helper in DataStoreValue
      } catch (Exception e) {
        return RespUtility.buildErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value");
      }
    }

    if (!entryID.matches("\\d+-\\d+")) {
      return RespUtility.buildErrorResponse("ERR invalid stream ID format");
    }

    if (stream.containsKey(entryID)) {
      return RespUtility.buildErrorResponse("ERR duplicate ID");
    }

    // Insert entry
    stream.put(entryID, entryValue);
    return RespUtility.serializeResponse(entryID);
  }
}
