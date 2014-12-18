
#pragma once

#include <nstd/Thread.h>
#include <nstd/Mutex.h>
#include <nstd/List.h>
#include <nstd/Buffer.h>

#include "Tools/Socket.h"

#include "DataProtocol.h"

class Client
{
public:
  Client(): nextRequestId(1), selectedTable(0) {}
  ~Client() {disconnect();}

  String getLastError() const {return error;}

  bool_t connect(const String& user, const String& password, const String& address);

  void_t disconnect();

  void_t list();
  void_t select(uint32_t tableId);
  void_t query();

private:
  enum ActionType
  {
    listAction,
    selectAction,
    queryAction,
  };
  struct Action
  {
    ActionType type;
    uint32_t param;
  };

private:
  static uint_t threadProc(void_t* param);

  uint8_t process();

  void_t handleData(const DataProtocol::Header& header);
  void_t handleAction(const Action& action);

  void_t interrupt();
  bool_t sendRequest(DataProtocol::Header& header);
  bool_t receiveData(Buffer& buffer);
  bool_t receiveResponse(uint32_t requestId, Buffer& buffer);

private:
  String error;
  Thread thread;
  Socket socket;
  Socket eventPipeRead;
  Socket eventPipeWrite;
  Mutex actionMutex;
  List<Action> actions;
  uint32_t nextRequestId;
  uint32_t selectedTable;
};
