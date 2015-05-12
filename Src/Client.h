
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

  void_t listUsers() {enqueueAction(listUsersAction);}
  void_t addUser(const String& userName, const String& password) {enqueueAction(addUserAction, userName, password);}
  void_t listTables() {enqueueAction(listTablesAction);}
  void_t createTable(const String& name) {enqueueAction(createTableAction, name);}
  void_t removeTable() {enqueueAction(removeTableAction);}
  void_t clearTable() {enqueueAction(clearTableAction);}
  void_t selectTable(uint32_t tableId) {enqueueAction(selectTableAction, tableId);}
  void_t query() {enqueueAction(queryAction);}
  void_t query(uint64_t sinceId) {enqueueAction(queryAction, sinceId);}
  void_t add(const String& value) {enqueueAction(addAction, value);}
  void_t subscribe() {enqueueAction(subscribeAction);}
  void_t sync() {enqueueAction(syncAction);}

private:
  enum ActionType
  {
    quitAction,
    listUsersAction,
    addUserAction,
    listTablesAction,
    createTableAction,
    removeTableAction,
    clearTableAction,
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
  static void_t zlimdbCallback(void_t* userData, const void_t* data) {((Client*)userData)->zlimdbCallback(data);}

  void_t enqueueAction(ActionType type, const Variant& param1 = Variant(), const Variant& param2 = Variant());

  void_t zlimdbCallback(const void_t* data);

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
