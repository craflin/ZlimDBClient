
#pragma once

#include <nstd/Thread.h>
#include <nstd/Mutex.h>
#include <nstd/List.h>
#include <nstd/Buffer.h>
#include <nstd/Variant.h>

typedef struct _zlimdb zlimdb;

class Client
{
public:
  Client();
  ~Client();

  String getLastError() const {return error;}

  bool_t connect(const String& userName, const String& password, const String& address);

  void_t disconnect();

  void_t listUsers();
  void_t addUser(const String& userName, const String& password);
  void_t listTables();
  void_t createTable(const String& name);
  void_t selectTable(uint32_t tableId);
  void_t query();
  void_t add(const String& value);
  void_t subscribe();
  void_t sync();

private:
  enum ActionType
  {
    quitAction,
    listUsersAction,
    addUserAction,
    listTablesAction,
    createTableAction,
    selectTableAction,
    queryAction,
    addAction,
    subscribeAction,
    syncAction,
  };
  struct Action
  {
    ActionType type;
    Variant param1;
    Variant param2;
  };

private:
  static uint_t threadProc(void_t* param);
  static void_t zlimdbCallback(void_t* userData, void_t* data, uint16_t size) {((Client*)userData)->zlimdbCallback(data, size);}

  void_t zlimdbCallback( void_t* data, uint16_t size);

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

private:
  static String getZlimdbError();
};
