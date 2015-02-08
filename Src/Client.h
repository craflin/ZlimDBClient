
#pragma once

#include <nstd/Thread.h>
#include <nstd/Mutex.h>
#include <nstd/List.h>
#include <nstd/Buffer.h>

#include "DataProtocol.h" // todo: remove this

typedef struct _zlimdb zlimdb;

class Client
{
public:
  Client() : zdb(0), selectedTable(0) {}
  ~Client() {disconnect();}

  String getLastError() const {return error;}

  bool_t connect(const String& user, const String& password, const String& address);

  void_t disconnect();

  void_t listTables();
  void_t createTable(const String& name);
  void_t selectTable(uint32_t tableId);
  void_t query();
  void_t add(const String& value);
  void_t subscribe();

private:
  enum ActionType
  {
    listTablesAction,
    createTableAction,
    selectTableAction,
    queryAction,
    addAction,
    subscribeAction,
  };
  struct Action
  {
    ActionType type;
    uint32_t param;
    String paramStr;
  };

private:
  static uint_t threadProc(void_t* param);
  static void_t zlimdbCallback(void_t* userData, zlimdb_message_type message_type, void_t* data, uint16_t size);

  uint8_t process();

  void_t handleAction(const Action& action);

private:
  String error;
  zlimdb* zdb;
  volatile bool keepRunning;
  Thread thread;
  Mutex actionMutex;
  List<Action> actions;
  uint32_t selectedTable;
};
