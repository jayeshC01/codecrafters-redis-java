package processors;

import db.DataStore;
import models.DataStoreValue;
import models.RespCommand;
import processors.lists.BLPOPExecutor;
import processors.lists.LPUSHExecutor;
import processors.lists.RPUSHExecutor;
import processors.streams.XADDExecutor;
import processors.streams.XRANGEExecutor;
import processors.streams.XREADExecutor;
import utility.RespUtility;

import java.util.*;
import java.util.stream.Collectors;

public class CommandProcessor {
  private boolean isTransactionEnabled = false;
  private final List<RespCommand> queuedCommands = new ArrayList<>();

  public CommandProcessor(){}

  public String processCommand(RespCommand cmd) {
    // Handle transaction mode first
    if (isTransactionEnabled && !cmd.getName().equalsIgnoreCase("DISCARD")
        && !cmd.getName().equalsIgnoreCase("EXEC")) {
      return queueCommands(cmd);
    }

    return switch (cmd.getName().toUpperCase()) {
      case "PING" -> RespUtility.buildSimpleResponse("PONG");
      case "ECHO" -> processEcho(cmd);
      case "SET" -> processSet(cmd);
      case "GET" -> processCommandGet(cmd);
      case "INCR" -> processCommandIncr(cmd);
      case "MULTI" -> processCommandMulti();
      case "EXEC" -> processCommandExec();
      case "DISCARD" -> processCommandDiscard();
      case "TYPE" -> processType(cmd);
      case "LLEN" -> processCommandLlen(cmd);
      case "LPOP" -> processCommandLpop(cmd);
      case "LRANGE" -> processCommandLrange(cmd);
      case "RPUSH" -> new RPUSHExecutor().execute(cmd);
      case "LPUSH" -> new LPUSHExecutor().execute(cmd);
      case "BLPOP" -> new BLPOPExecutor().execute(cmd);
      case "XADD" -> new XADDExecutor().execute(cmd);
      case "XRANGE" -> new XRANGEExecutor().execute(cmd);
      case "XREAD" -> new XREADExecutor().execute(cmd);
      default -> RespUtility.buildErrorResponse("Invalid Command: " + cmd);
    };
  }

  private String processType(RespCommand cmd) {
    if(cmd.getArgsSize() != 1) {
      return RespUtility.buildErrorResponse("Invalid arguments");
    }
    String key = cmd.getArgs().get(0);
    DataStoreValue data = DataStore.get(key);
    return RespUtility.buildSimpleResponse(data == null ? "none" : data.getValueType());
  }

  private String processCommandLpop(RespCommand cmd) {
    List<String> args = cmd.getArgs();
    if(cmd.getArgsSize() < 1 || cmd.getArgsSize() >=3) {
      return RespUtility.buildErrorResponse("Invalid arguments to LPOP");
    }
    DataStoreValue data = DataStore.get(args.get(0));
    if(data == null) {
      return RespUtility.serializeResponse(null);
    }
    try {
      //Pop Single element
      if(args.size() == 1) {
        return RespUtility.serializeResponse(data.getAsLinkedList().pollFirst());
      }
      //Pop multiple elements
      if(args.size() == 2) {
        List<String> responses = new ArrayList();
        for (int i = 0; i < Integer.parseInt(args.get(1)) && !data.getAsLinkedList().isEmpty(); i++) {
          responses.add(data.getAsLinkedList().pollFirst());
        }
        return RespUtility.serializeResponse(responses);
      }

      //Fallback
      return RespUtility.buildErrorResponse("Unexpected error in LPOP");
    } catch (Exception e) {
      return RespUtility.buildErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
  }

  private String processCommandLlen(RespCommand cmd) {
    if (cmd.getArgsSize() != 1) {
      return RespUtility.buildErrorResponse("ERR wrong number of arguments for 'LLEN' command");
    }

    DataStoreValue data = DataStore.get(cmd.getArgs().get(0));
    if(data == null) {
      return RespUtility.serializeResponse(0);
    }
    try {
      return RespUtility.serializeResponse(data.getAsList().size());
    } catch (Exception e) {
      return RespUtility.buildErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
  }





  private String processCommandLrange(RespCommand cmd) {
    String key = cmd.getArgs().get(0);
    List<String> args = cmd.getArgs().subList(1, cmd.getArgsSize());
    DataStoreValue data = DataStore.get(key);

    if(data == null) {
      return RespUtility.serializeResponse(Collections.emptyList());
    }
    try {
      List<String> element = data.getAsList();
      int size = element.size();
      int start = Integer.parseInt(args.get(0));
      int end = Integer.parseInt(args.get(1));
      // Normalize negative indexes
      if (start < 0) start = size + start;
      if (end < 0) end = size + end;
      // Calculating Valid Range
      start = Math.max(0, start);
      end = Math.min(end, size - 1);
      if (start > end || start >= size) {
        return RespUtility.serializeResponse(Collections.emptyList());
      }
      List<String> response = element.subList(start, end + 1);
      return RespUtility.serializeResponse(response);
    } catch (Exception e) {
      return RespUtility.buildErrorResponse("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
  }

  private String processEcho(RespCommand cmd) {
    if (cmd.getArgsSize() !=1 ) {
      return RespUtility.buildErrorResponse("invalid command ECHO - wrong number of arguments");
    }
    return RespUtility.serializeResponse(cmd.getArgs().get(0));
  }

  private String processCommandGet(RespCommand cmd) {
    if (cmd.getArgsSize() != 1) {
      return RespUtility.buildErrorResponse("invalid command get - wrong number of arguments");
    }
    DataStoreValue data = DataStore.get(cmd.getArgs().get(0));
    if(data!=null && !data.isExpired()) {
      return RespUtility.serializeResponse(data.getAsString());
    }
    return RespUtility.serializeResponse(null);
  }

  private String processSet(RespCommand cmd) {
    List<String> args = cmd.getArgs();

    if (args.size() < 2 || args.size() > 4) {
      return RespUtility.buildErrorResponse("wrong number of arguments for 'SET' command");
    }

    String key = args.get(0);
    String value = args.get(1);
    String option = args.size() >= 3 ? args.get(2).toUpperCase() : null;
    String expiryArg = args.size() == 4 ? args.get(3) : null;
    long expiryMillis = 0;

    if ("EX".equals(option) || "PX".equals(option)) {
      if (expiryArg == null) {
        return RespUtility.buildErrorResponse("missing expiry time for EX/PX option");
      }

      try {
        long expiryTime = Long.parseLong(expiryArg);
        expiryMillis = System.currentTimeMillis() + (
            "EX".equals(option) ? expiryTime * 1000 : expiryTime
        );
      } catch (NumberFormatException e) {
        return RespUtility.buildErrorResponse("invalid expiry time - must be a number");
      }
    }

    if ("NX".equals(option)) {
      if (DataStore.containsKey(key)) {
        return RespUtility.serializeResponse(null); // don't overwrite existing
      }
    } else if ("XX".equals(option)) {
      if (!DataStore.containsKey(key)) {
        return RespUtility.serializeResponse(null); // don't set if absent
      }
    } else if (option != null && !"EX".equals(option) && !"PX".equals(option)) {
      return RespUtility.buildErrorResponse("unknown option: " + option);
    }

    DataStore.put(key, new DataStoreValue(value, expiryMillis));
    return RespUtility.buildSimpleResponse("OK");
  }

  private String processCommandIncr(RespCommand cmd) {
    if (cmd.getArgsSize() != 1) {
      return RespUtility.buildErrorResponse("wrong number of arguments for 'INCR' command");
    }

    String key = cmd.getArgs().get(0);
    DataStoreValue data = DataStore.get(key);

    if (data == null || data.isExpired()) {
      DataStore.put(key, new DataStoreValue(String.valueOf(1)));
      return RespUtility.serializeResponse(1);
    }

    try {
      long existingValue = data.getAsLong();
      data.updateValue(String.valueOf(existingValue + 1));
      DataStore.put(key, data);
      return RespUtility.serializeResponse(existingValue + 1);
    } catch (NumberFormatException e) {
      return RespUtility.buildErrorResponse("value is not an integer or out of range");
    }
  }


  private String processCommandMulti() {
    isTransactionEnabled = true;
    return RespUtility.buildSimpleResponse("OK");
  }

  private String processCommandExec() {
    if(!isTransactionEnabled) {
      return RespUtility.buildErrorResponse("EXEC without MULTI");
    }
    isTransactionEnabled = false;
    if(queuedCommands.isEmpty()) {
      return RespUtility.serializeResponse(List.of());
    }
    List<String> responses = queuedCommands.stream()
        .map(this::processCommand)
        .collect(Collectors.toList());
    queuedCommands.clear();
    return RespUtility.serializeResponse(responses);
  }

  private String queueCommands(RespCommand cmd) {
    if (!cmd.getName().equalsIgnoreCase("EXEC")) {
      queuedCommands.add(cmd);
      return RespUtility.buildSimpleResponse("QUEUED");
    }
    return processCommandExec();
  }

  private String processCommandDiscard() {
    if(isTransactionEnabled){
      queuedCommands.clear();
      isTransactionEnabled=false;
      return RespUtility.buildSimpleResponse("OK");
    }
    return RespUtility.buildErrorResponse("DISCARD without MULTI");
  }
}
