
#include <nstd/Console.h>
#include <nstd/Error.h>
#include <nstd/Time.h>

#include <lz4.h>

#include "Tools/Sha256.h"

#include "Client.h"

bool_t Client::connect(const String& user, const String& password, const String& address)
{
  disconnect();

  // establish connection
  uint16_t port;
  uint32_t ipAddr = Socket::resolveAddr(address, port);
  if(ipAddr == Socket::noneAddr)
  {
    error = Error::getErrorString();
    return false;
  }
  if(!socket.open() || !socket.connect(ipAddr, port))
  {
    error = Error::getErrorString();
    return false;
  }

  // send login request
  Buffer buffer(sizeof(DataProtocol::LoginRequest) + user.length());
  DataProtocol::LoginRequest* loginRequest = (DataProtocol::LoginRequest*)(byte_t*)buffer;
  loginRequest->messageType = DataProtocol::loginRequest;
  loginRequest->size = sizeof(DataProtocol::LoginRequest) + user.length();
  loginRequest->requestId = nextRequestId++;
  loginRequest->userNameSize = user.length();
  Memory::copy(loginRequest + 1, user, user.length());
  if(!sendRequest(*loginRequest))
    return false;

  // receive login response
  if(!receiveResponse(loginRequest->requestId , buffer))
    return false;
  const DataProtocol::LoginResponse* loginResponse = (const DataProtocol::LoginResponse*)(const byte_t*)buffer;
  if(loginResponse->messageType != DataProtocol::loginResponse || loginResponse->size < sizeof(*loginResponse))
  {
    error = "Received invalid login response.";
    return false;
  }

  // send auth request
  DataProtocol::AuthRequest authRequest;
  authRequest.messageType = DataProtocol::authRequest;
  authRequest.size = sizeof(authRequest);
  authRequest.requestId = nextRequestId++;
  byte_t pwHash[32];
  Sha256::hmac(loginResponse->pwSalt, sizeof(loginResponse->pwSalt), (const byte_t*)(const char_t*)password, password.length(), pwHash);
  Sha256::hmac(loginResponse->authSalt, sizeof(loginResponse->authSalt), pwHash, sizeof(pwHash), authRequest.signature);
  if(!sendRequest(authRequest))
    return false;

  // receive auth response
  if(!receiveResponse(authRequest.requestId, buffer))
    return false;
  const DataProtocol::Header* authResponse = (const DataProtocol::Header*)(const byte_t*)buffer;
  if(authResponse->messageType != DataProtocol::authResponse)
  {
    error = "Received invalid auth response.";
    return false;
  }

  // create event socket pair
  if(!eventPipeRead.pair(eventPipeWrite))
    return false;

  // start receive thread
  if(!thread.start(threadProc, this))
  {
    error = Error::getErrorString();
    return false;
  }
  return true;
}

void_t Client::disconnect()
{
  eventPipeWrite.close(); // terminate worker thread
  thread.join();
  socket.close();
  eventPipeRead.close();
  actions.clear();
  nextRequestId = 1;
  selectedTable = 0;
}

void_t Client::listTables()
{
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = listTablesAction;
  actionMutex.unlock();
  interrupt();
}

void_t Client::createTable(const String& name)
{
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = createTableAction;
  action.paramStr = name;
  actionMutex.unlock();
  interrupt();
}

void_t Client::selectTable(uint32_t tableId)
{
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = selectTableAction;
  action.param = tableId;
  actionMutex.unlock();
  interrupt();
}

void_t Client::query()
{
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = queryAction;
  actionMutex.unlock();
  interrupt();
}

void_t Client::add(const String& value)
{
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = addAction;
  action.paramStr = value;
  actionMutex.unlock();
  interrupt();
}

void_t Client::subscribe()
{
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = subscribeAction;
  actionMutex.unlock();
  interrupt();
}

uint_t Client::threadProc(void_t* param)
{
  Client* client = (Client*)param;
  return client->process();;
}

uint8_t Client::process()
{
  Socket::Selector selector;
  selector.set(socket, Socket::Selector::readEvent);
  selector.set(eventPipeRead, Socket::Selector::readEvent);
  Buffer buffer;
  Socket* selectedSocket;
  uint_t events;
  while(socket.isOpen())
  {
    if(!selector.select(selectedSocket, events, 100 * 1000))
      continue;
    if(selectedSocket == &socket)
    {
      if(!receiveData(buffer))
      { // connection lost or error
          Console::errorf("error: Could not receive data: %s\n", (const char_t*)error);
          return 1;
      }
      handleData(*(const DataProtocol::Header*)(const byte_t*)buffer);
    }
    else
    {
      uint32_t event;
      switch(eventPipeRead.recv((byte_t*)&event, sizeof(event), sizeof(event)))
      {
      case 0:
        return 0;
      case sizeof(event):
        {
          Action action;
          actionMutex.lock();
          action = actions.front();
          actions.removeFront();
          actionMutex.unlock();
          handleAction(action);
        }
        break;
      default: // unexpected result
        return 1; 
      }
    }
  }
  return false;
}

void_t Client::handleData(const DataProtocol::Header& header)
{
  //switch(header.messageType)
  //{
  //case DataProtocol::queryResponse:
  //  handleQueryResponse(header);
  //  break;
  //}
}

void_t Client::handleAction(const Action& action)
{
  switch(action.type)
  {
  case listTablesAction:
    {
      DataProtocol::QueryRequest queryRequest;
      queryRequest.messageType = DataProtocol::queryRequest;
      queryRequest.size = sizeof(queryRequest);
      queryRequest.tableId = DataProtocol::tablesTable;
      queryRequest.type = DataProtocol::QueryRequest::all;
      queryRequest.requestId = nextRequestId++;
      if(!sendRequest(queryRequest))
      {
        Console::errorf("error: Could not send query: %s\n", (const char_t*)error);
        return;
      }
      Buffer buffer;
      for(;;)
      {
        if(!receiveResponse(queryRequest.requestId, buffer))
        {
          Console::errorf("error: Could not receive query response: %s\n", (const char_t*)error);
          return;
        }
        const DataProtocol::Header* header = (const DataProtocol::Header*)(const byte_t*)buffer;
        if(header->messageType != DataProtocol::queryResponse)
        {
          error = "Receive invalid query response.";
          Console::errorf("error: Could not receive query response: %s\n", (const char_t*)error);
          return;
        }

        for(const DataProtocol::Table* table = (const DataProtocol::Table*)(header + 1), * end = (const DataProtocol::Table*)((const byte_t*)header + header->size); table < end; table = (const DataProtocol::Table*)((const byte_t*)table + table->size))
        {
          String tableName;
          DataProtocol::getString(*header, *table, sizeof(DataProtocol::Table), table->nameSize, tableName);
          tableName.resize(tableName.length());
          Console::printf("%6llu: %s\n", table->id, (const char_t*)tableName);
        }

        if(!(header->flags & DataProtocol::Header::fragmented))
          break;
      }
    }
    break;
  case selectTableAction:
    selectedTable = action.param;
    //Console::printf("selected table %u\n", action.param);
    break;
  case createTableAction:
    {
      const String& tableName = action.paramStr;
      Buffer buffer;
      buffer.resize(sizeof(DataProtocol::AddRequest) + sizeof(DataProtocol::Table) + tableName.length());
      DataProtocol::AddRequest* addRequest = (DataProtocol::AddRequest*)(byte_t*)buffer;
      DataProtocol::setHeader(*addRequest, DataProtocol::addRequest, buffer.size(), nextRequestId++);
      addRequest->tableId = DataProtocol::tablesTable;
      DataProtocol::Table* table = (DataProtocol::Table*)(addRequest + 1);
      DataProtocol::setEntityHeader(*table, 0, 0, sizeof(*table) + tableName.length());
      table->flags = 0;
      DataProtocol::setString(*table, table->nameSize, sizeof(*table), tableName);
      if(!sendRequest(*addRequest))
      {
        Console::errorf("error: Could not send add request: %s\n", (const char_t*)error);
        return;
      }
      if(!receiveResponse(addRequest->requestId, buffer))
      {
        Console::errorf("error: Could not receive add response: %s\n", (const char_t*)error);
        return;
      }
    }
    break;
  case queryAction:
  case subscribeAction:
    {
      DataProtocol::SubscribeRequest subscribeRequest;
      subscribeRequest.messageType = action.type == queryAction ? DataProtocol::queryRequest : DataProtocol::subscribeRequest;
      subscribeRequest.size = sizeof(subscribeRequest);
      subscribeRequest.requestId = nextRequestId++;
      subscribeRequest.tableId = selectedTable;
      subscribeRequest.type = DataProtocol::SubscribeRequest::all;
      subscribeRequest.param = 0;
      if(!sendRequest(subscribeRequest))
      {
        if(action.type == queryAction)
          Console::errorf("error: Could not send query: %s\n", (const char_t*)error);
        else
          Console::errorf("error: Could not send subscribe request: %s\n", (const char_t*)error);
        return;
      }
      Buffer buffer;
      for(;;)
      {
        if(!receiveResponse(subscribeRequest.requestId, buffer))
        {
          if(action.type == queryAction)
            Console::errorf("error: Could not receive query response: %s\n", (const char_t*)error);
          else
            Console::errorf("error: Could not receive subscribe response: %s\n", (const char_t*)error);
          return;
        }
        const DataProtocol::Header* header = (const DataProtocol::Header*)(const byte_t*)buffer;
        if(header->messageType != DataProtocol::queryResponse || header->size < sizeof(*header) + sizeof(uint16_t))
        {
          if(action.type == queryAction)
            error = "Received invalid query response.";
          else
            error = "Received invalid subscribe response.";
          if(action.type == queryAction)
            Console::errorf("error: Could not receive query response: %s\n", (const char_t*)error);
          else
            Console::errorf("error: Could not receive subscribe response: %s\n", (const char_t*)error);
          return;
        }

        const byte_t* data;
        size_t dataSize;
        Buffer buffer;
        if(header->flags & DataProtocol::Header::compressed)
        {
          dataSize = *(const uint16_t*)(header + 1);
          uint16_t compressedSize = header->size - (sizeof(*header) + sizeof(uint16_t));
          buffer.resize(dataSize);
          if(dataSize > 0 && LZ4_decompress_safe((const char*)(header + 1) + sizeof(uint16_t), (char*)(byte_t*)buffer, compressedSize, dataSize) != dataSize)
          {
            error = "Decompression failed.";
            if(action.type == queryAction)
              Console::errorf("error: Could not receive query response: %s\n", (const char_t*)error);
            else
              Console::errorf("error: Could not receive subscribe response: %s\n", (const char_t*)error);
            return;
          }
          data = buffer;
        }
        else
        {
          data = (const byte_t*)(header + 1);
          dataSize = header->size - sizeof(*header);
        }

        for(const DataProtocol::Entity* entity = (const DataProtocol::Entity*)data, * end = (const DataProtocol::Entity*)((const byte_t*)data + dataSize); entity < end; entity = (const DataProtocol::Entity*)((const byte_t*)entity + entity->size))
        {
          Console::printf("id=%llu, size=%u, time=%llu\n", entity->id, (uint_t)entity->size, entity->time);
        }
        if(!(header->flags & DataProtocol::Header::fragmented))
          break;
      }
    }
    break;
  case addAction:
    {
      const String& value = action.paramStr;
      Buffer buffer;
      buffer.resize(sizeof(DataProtocol::AddRequest) + sizeof(DataProtocol::Table) + value.length());
      DataProtocol::AddRequest* addRequest = (DataProtocol::AddRequest*)(byte_t*)buffer;
      DataProtocol::setHeader(*addRequest, DataProtocol::addRequest, buffer.size(), nextRequestId++);
      addRequest->tableId = selectedTable;
      DataProtocol::Table* entity = (DataProtocol::Table*)(addRequest + 1);
      DataProtocol::setEntityHeader(*entity, 0, Time::time(), sizeof(DataProtocol::Table) + value.length());
      DataProtocol::setString(*entity, entity->nameSize, sizeof(*entity), value);
      if(!sendRequest(*addRequest))
      {
        Console::errorf("error: Could not send add request: %s\n", (const char_t*)error);
        return;
      }
      if(!receiveResponse(addRequest->requestId, buffer))
      {
        Console::errorf("error: Could not receive add response: %s\n", (const char_t*)error);
        return;
      }
    }
    break;
  }
}

void_t Client::interrupt()
{
  uint32_t event = 1;
  eventPipeWrite.send((byte_t*)&event, sizeof(event));
}

bool_t Client::sendRequest(DataProtocol::Header& header)
{
  header.flags = 0;
  header.requestId = 0;
  if(socket.send((const byte_t*)&header, header.size) != header.size)
  {
    error = Error::getErrorString();
    return false;
  }
  return true;
}

bool_t Client::receiveData(Buffer& buffer)
{
  buffer.resize(sizeof(DataProtocol::Header));
  ssize_t x = socket.recv((byte_t*)buffer, sizeof(DataProtocol::Header), sizeof(DataProtocol::Header));
  if(x != sizeof(DataProtocol::Header))
  {
    if(Error::getLastError())
      error = Error::getErrorString();
    else
      error = "Connection closed by peer.";
    socket.close();
    return false;
  }
  const DataProtocol::Header* header = (const DataProtocol::Header*)(const byte_t*)buffer;
  if(header->size < sizeof(DataProtocol::Header))
  {
    error = "Received invalid data.";
    return false;
  }
  size_t dataSize = header->size - sizeof(DataProtocol::Header);
  if(dataSize > 0)
  {
    buffer.resize(header->size);
    if(socket.recv((byte_t*)buffer + sizeof(DataProtocol::Header), dataSize, dataSize) != dataSize)
    {
      if(Error::getLastError())
        error = Error::getErrorString();
      else
        error = "Connection closed by peer.";
      socket.close();
      return false;
    }
  }
  return true;
}

bool_t Client::receiveResponse(uint32_t requestId, Buffer& buffer)
{
  while(socket.isOpen())
  {
    if(!receiveData(buffer))
      return false;
    const DataProtocol::Header* header = (const DataProtocol::Header*)(const byte_t*)buffer;
    if(header->requestId == requestId)
    {
      if(header->messageType == DataProtocol::errorResponse && header->size >= sizeof(DataProtocol::ErrorResponse))
      {
        error = DataProtocol::getErrorString((DataProtocol::Error)((const DataProtocol::ErrorResponse*)header)->error);
        return false;
      }
      return true;
    }
    else
      handleData(*header);
  }
  return false;
}
