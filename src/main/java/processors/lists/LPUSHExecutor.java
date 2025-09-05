package processors.lists;

import db.DataStore;
import models.DataStoreValue;
import models.RespCommand;
import processors.CommandExecutor;
import utility.RespUtility;

import java.util.LinkedList;
import java.util.List;

public class LPUSHExecutor implements CommandExecutor {

  @Override
  public String execute(RespCommand cmd) {
    String key = cmd.getKey();
    DataStoreValue data = DataStore.get(key);
    List<String> elementsToPush = cmd.getArgs().subList(1, cmd.getArgsSize());
    if(data == null) {
      DataStore.put(key, new DataStoreValue(elementsToPush));
      DataStore.notifyWaiter(key);
      return RespUtility.serializeResponse(cmd.getArgsSize()-1);
    }

    LinkedList<String> existingList = new LinkedList<>(data.getAsList());
    elementsToPush.forEach(existingList::addFirst);
    data.updateValue(existingList);
    DataStore.notifyWaiter(key);
    return RespUtility.serializeResponse(data.getAsList().size());
  }
}
