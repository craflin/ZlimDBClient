
#pragma once

#include <zlimdbprotocol.h>

#include <nstd/Base.h>

class ClientProtocol
{
public:
  static bool_t getString(const zlimdb_entity& entity, size_t offset, size_t length, String& result)
  {
    if(offset + length > entity.size)
      return false;
    result.attach((const char_t*)&entity + offset, length);
    return true;
  }

  //static void_t setHeader(Header& header, MessageType type, size_t size, uint32_t requestId, uint8_t flags = 0)
  //{
  //  header.size = size;
  //  header.messageType = type;
  //  header.requestId = requestId;
  //  header.flags = flags;
  //}
  //
  static void_t setEntityHeader(zlimdb_entity& entity, uint64_t id, uint64_t time, uint16_t size)
  {
    entity.id = id;
    entity.time = time;
    entity.size = size;
  }

  static void_t setString(zlimdb_entity& entity, uint16_t& length, size_t offset, const String& str)
  {
    length = str.length();
    Memory::copy((byte_t*)&entity + offset, (const char_t*)str, str.length());
  }
};
