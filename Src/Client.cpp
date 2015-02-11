
#include <nstd/Console.h>
#include <nstd/Error.h>
#include <nstd/Time.h>
#include <nstd/Debug.h>

#include <zlimdbclient.h>

#include "Client.h"
#include "ClientProtocol.h"

Client::Client() : zdb(0), selectedTable(0)
{
  VERIFY(zlimdb_init() == 0);
}

Client::~Client()
{
  disconnect();
  VERIFY(zlimdb_cleanup() == 0);
}

bool_t Client::connect(const String& user, const String& password, const String& address)
{
  disconnect();

  // create connection
  zdb = zlimdb_create(zlimdbCallback, this);
  if(!zdb)
  {
    error.printf("%s.", zlimdb_strerror(zlimdb_errno()));
    return false;
  }
  uint16_t port = 0;
  String host = address;
  const char_t* colon = address.find(':');
  if(colon)
  {
    port = String::toUInt(colon + 1);
    host = address.substr(0, colon - (const char_t*)address);
  }
  if(zlimdb_connect(zdb, host, port, user, password) != 0)
  {
    if(zlimdb_errno() == zlimdb_local_error_socket)
      error = Error::getErrorString();
    else
      error.printf("%s.", zlimdb_strerror(zlimdb_errno()));
    return false;
  }

  // start receive thread
  keepRunning = true;
  if(!thread.start(threadProc, this))
  {
    error = Error::getErrorString();
    return false;
  }
  return true;
}

void_t Client::disconnect()
{
  if(zdb)
  {
    keepRunning = false;
    zlimdb_interrupt(zdb);
  }
  thread.join();
  actions.clear();
  selectedTable = 0;
}

void_t Client::listTables()
{
  if(!zdb)
    return;
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = listTablesAction;
  actionMutex.unlock();
  zlimdb_interrupt(zdb);
}

void_t Client::createTable(const String& name)
{
  if(!zdb)
    return;
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = createTableAction;
  action.paramStr = name;
  actionMutex.unlock();
  zlimdb_interrupt(zdb);
}

void_t Client::selectTable(uint32_t tableId)
{
  if(!zdb)
    return;
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = selectTableAction;
  action.param = tableId;
  actionMutex.unlock();
  zlimdb_interrupt(zdb);
}

void_t Client::query()
{
  if(!zdb)
    return;
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = queryAction;
  actionMutex.unlock();
  zlimdb_interrupt(zdb);
}

void_t Client::add(const String& value)
{
  if(!zdb)
    return;
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = addAction;
  action.paramStr = value;
  actionMutex.unlock();
  zlimdb_interrupt(zdb);
}

void_t Client::subscribe()
{
  if(!zdb)
    return;
  actionMutex.lock();
  Action& action = actions.append(Action());
  action.type = subscribeAction;
  actionMutex.unlock();
  zlimdb_interrupt(zdb);
}

uint_t Client::threadProc(void_t* param)
{
  Client* client = (Client*)param;
  return client->process();;
}

uint8_t Client::process()
{
  while(keepRunning)
    if(zlimdb_exec(zdb, 5 * 60 * 1000) != 0)
      switch(zlimdb_errno())
      {
      case zlimdb_local_error_interrupted:
        {
          Action action = {quitAction};
          bool actionEmpty = true;
          do
          {
            actionMutex.lock();
            if(!actions.isEmpty())
            {
              action = actions.front();
              actions.removeFront();
              actionEmpty = actions.isEmpty();
            }
            actionMutex.unlock();
            handleAction(action);
          } while(!actionEmpty);
        }
        break;
      case zlimdb_local_error_timeout:
        break;
      default:
        //Console::errorf("error: Could not receive data: %s\n", zlimdb_strerror(zlimdb_errno()));
        return 1;
      }
  return 0;
}

void_t Client::handleAction(const Action& action)
{
  switch(action.type)
  {
  case listTablesAction:
    {
      if(zlimdb_query(zdb, zlimdb_table_tables, zlimdb_query_type_all, 0) != 0)
      {
        Console::errorf("error: Could not send query: %s\n", zlimdb_strerror(zlimdb_errno()));
        return;
      }
      Buffer buffer;
      buffer.resize(0xffff);
      uint32_t size;
      while(zlimdb_get_response(zdb, buffer, buffer.size(), &size) == 0)
      {
        for(const zlimdb_table_entity* table = (const zlimdb_table_entity*)(const byte_t*)buffer, * end = (const zlimdb_table_entity*)((const byte_t*)table + size); table < end; table = (const zlimdb_table_entity*)((const byte_t*)table + table->entity.size))
        {
          String tableName;
          ClientProtocol::getString((const byte_t*)buffer, size, table->entity, sizeof(zlimdb_table_entity), table->name_size, tableName);
          tableName.resize(tableName.length()); // enfore NULL termination
          Console::printf("%6llu: %s\n", table->entity.id, (const char_t*)tableName);
        }
      }
      if(zlimdb_errno() != zlimdb_local_error_none)
      {
        Console::errorf("error: Could not receive query response: %s\n", zlimdb_strerror(zlimdb_errno()));
        return;
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
      buffer.resize(sizeof(zlimdb_table_entity) + tableName.length());
      zlimdb_table_entity* table = (zlimdb_table_entity*)(byte_t*)buffer;
      ClientProtocol::setEntityHeader(table->entity, 0, 0, sizeof(*table) + tableName.length());
      table->flags = 0;
      ClientProtocol::setString(table->entity, table->name_size, sizeof(*table), tableName);
      if(zlimdb_add(zdb, zlimdb_table_tables, (const byte_t*)buffer, buffer.size()) != 0)
      {
        Console::errorf("error: Could not send add request: %s\n",zlimdb_strerror(zlimdb_errno()));
        return;
      }
    }
    break;
  case queryAction:
    {
      if(zlimdb_query(zdb, selectedTable, zlimdb_query_type_all, 0) != 0)
      {
        Console::errorf("error: Could not send query: %s\n", zlimdb_strerror(zlimdb_errno()));
        return;
      }
      Buffer buffer;
      buffer.resize(0xffff);
      uint32_t size;
      while(zlimdb_get_response(zdb, buffer, buffer.size(), &size) == 0)
      {
        for(const zlimdb_entity* entity = (const zlimdb_entity*)(const byte_t*)buffer, * end = (const zlimdb_entity*)((const byte_t*)entity + size); entity < end; entity = (const zlimdb_entity*)((const byte_t*)entity + entity->size))
        {
          Console::printf("id=%llu, size=%u, time=%llu\n", entity->id, (uint_t)entity->size, entity->time);
        }
      }
      if(zlimdb_errno() != zlimdb_local_error_none)
      {
        Console::errorf("error: Could not receive query response: %s\n", zlimdb_strerror(zlimdb_errno()));
        return;
      }
    }
    break;
  case subscribeAction:
    {
      if(zlimdb_subscribe(zdb, selectedTable, zlimdb_query_type_all, 0) != 0)
      {
        Console::errorf("error: Could not send subscribe request: %s\n", zlimdb_strerror(zlimdb_errno()));
        return;
      }
      Buffer buffer;
      buffer.resize(0xffff);
      uint32_t size;
      while(zlimdb_get_response(zdb, buffer, buffer.size(), &size) == 0)
      {
        for(const zlimdb_entity* entity = (const zlimdb_entity*)(const byte_t*)buffer, * end = (const zlimdb_entity*)((const byte_t*)entity + size); entity < end; entity = (const zlimdb_entity*)((const byte_t*)entity + entity->size))
        {
          Console::printf("id=%llu, size=%u, time=%llu\n", entity->id, (uint_t)entity->size, entity->time);
        }
      }
      if(zlimdb_errno() != zlimdb_local_error_none)
      {
        Console::errorf("error: Could not receive subscribe response: %s\n", zlimdb_strerror(zlimdb_errno()));
        return;
      }
    }
    break;
  case addAction:
    {
      const String& value = action.paramStr;
      Buffer buffer;
      buffer.resize(sizeof(zlimdb_table_entity) + value.length());
      zlimdb_table_entity* entity = (zlimdb_table_entity*)(const byte_t*)buffer;
      ClientProtocol::setEntityHeader(entity->entity, 0, Time::time(), sizeof(zlimdb_table_entity) + value.length());
      ClientProtocol::setString(entity->entity, entity->name_size, sizeof(*entity), value);
      if(zlimdb_add(zdb, selectedTable, buffer, buffer.size()))
      {
        Console::errorf("error: Could not send add request: %s\n", zlimdb_strerror(zlimdb_errno()));
        return;
      }
    }
    break;
  }
}

void_t Client::zlimdbCallback(void_t* data, uint16_t size)
{
  const zlimdb_header* header = (const zlimdb_header*)data;
  // todo: check sizes
  switch(header->message_type)
  {
  case zlimdb_message_error_response:
    {
      const zlimdb_error_response* errorResponse = (const zlimdb_error_response*)header;
      Console::printf("subscribe: errorResponse=%s (%d)\n", zlimdb_strerror(errorResponse->error), (int)errorResponse->error);
    }
    break;
  default:
    Console::printf("subscribe: messageType=%u\n", (uint_t)header->message_type);
    break;
  }
}
