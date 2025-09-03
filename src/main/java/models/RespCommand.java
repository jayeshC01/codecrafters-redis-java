package models;

import java.util.ArrayList;
import java.util.List;

public class RespCommand {
  String name;
  List<String> args;

  public RespCommand(String name, List<String> args) {
    this.name = name;
    this.args = args;
  }

  public List<String> getArgs() {
    return args;
  }

  public String getName() {
    return name.toUpperCase();
  }

  public int getArgsSize() {
    return args.size();
  }

  public boolean areArgsEmpty() {
    return args.isEmpty();
  }

  public String getKey(){
    if(this.areArgsEmpty()) {
      return null;
    }
    return args.get(0);
  }

  public String getStringRepresentation() {
    return name.concat(" ").concat(args.toString());
  }
}
