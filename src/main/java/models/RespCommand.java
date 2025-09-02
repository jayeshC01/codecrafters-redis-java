package models;

import java.util.List;

public class RespCommand {
  String name;
  List<String> args;

  public RespCommand(String name, List<String> args) {
    this.name = name;
    this.args = args;
  }

  public List<String> getArgs() {
    return this.args;
  }

  public String getName() {
    return this.name.toUpperCase();
  }

  public int getArgsSize() {
    return this.args.size();
  }

  public boolean areArgsEmpty() {
    return this.args.isEmpty();
  }

  public String getStringRepresentation() {
    return name.concat(" ").concat(args.toString());
  }
}
