package processors.lists;

import db.DataStore;
import models.DataStoreValue;
import models.RespCommand;
import processors.CommandExecutor;
import utility.RespUtility;

import java.util.LinkedList;
import java.util.List;

public class RPUSHExecutor implements CommandExecutor {

  @Override
  public String execute(RespCommand cmd) {
    String key = cmd.getKey();
    List<String> elementsToPush = cmd.getArgs().subList(1, cmd.getArgsSize());
    DataStoreValue data = DataStore.get(key);
    if(data == null) {
      DataStore.put(key, new DataStoreValue(new LinkedList<>(elementsToPush)));
      DataStore.notifyWaiter(key);
      return RespUtility.serializeResponse(cmd.getArgsSize()-1);
    }
    LinkedList<String> existingList = data.getAsLinkedList();
    existingList.addAll(elementsToPush);
    DataStore.notifyWaiter(key);
    return RespUtility.serializeResponse(data.getAsList().size());
  }
}
