
#include <nstd/Console.h>
#include <nstd/Process.h>

#include "Tools/Word.h"

#include "Client.h"

void_t help()
{
  Console::printf("list - Show list of tables.\n");
  Console::printf("query - Query data from selected table.\n");
  Console::printf("select <num> - Select a table for further requests.\n");
  Console::printf("exit - Quit the session.\n");
}

int_t main(int_t argc, char_t* argv[])
{
  String password("root");
  String user("root");
  String address("127.0.0.1:13211");
  {
    Process::Option options[] = {
        {'p', "password", Process::argumentFlag},
        {'u', "user", Process::argumentFlag},
    };
    Process::Arguments arguments(argc, argv, options);
    int_t character;
    String argument;
    while(arguments.read(character, argument))
      switch(character)
      {
      case 'p':
        password = argument;
        break;
      case 'u':
        user = argument;
        break;
      case 0:
        address = argument;
        break;
      case '?':
        Console::errorf("Unknown option: %s.\n", (const char_t*)argument);
        return 1;
      case ':':
        Console::errorf("Option %s required an argument.\n", (const char_t*)argument);
        return 1;
      default:
        Console::errorf("Usage: %s [-u <user>] [-p <password>] [<address>]\n");
        return 1;
      }
  }

  Console::Prompt prompt;
  Client client;
  if(!client.connect(user, password, address))
  {
    Console::errorf("error: Could not establish connection: %s\n", (const char_t*)client.getLastError());
    return 1;
  }
  for(;;)
  {
    String result = prompt.getLine("> ");
    Console::printf(String("> ") + result + "\n");

    List<String> args;
    Word::split(result, args);
    String cmd = args.isEmpty() ? String() : args.front();

    if(cmd == "exit" || cmd == "quit")
      break;
    else if(cmd == "help")
      help();
    else if(cmd == "list")
      client.list();
    else if(cmd == "select")
    {
      if(args.size() < 2)
        Console::errorf("error: Missing argument: select <num>\n");
      else
      {
        String num = *(++args.begin());
        client.select(num.toUInt());
      }
    }
    else if(cmd == "query")
    {
      client.query();
    }
    else if(!cmd.isEmpty())
      Console::errorf("error: Unkown command: %s\n", (const char_t*)result);
  }
  client.disconnect();
  return 0;
}
