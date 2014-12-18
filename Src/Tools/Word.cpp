
#include "Word.h"

size_t Word::split(const String& data, List<String>& result)
{
  const char_t* start = data;
  const char_t* str = start;
  for(;;)
  {
    while(String::isSpace(*str))
      ++str;
    if(!*str)
      break;
    if(*str == _T('"'))
    {
      ++str;
      const char* end = str;
      for(; *end; ++end)
        if(*end == _T('\\') && end[1] == _T('"'))
          ++end;
        else if(*end == _T('"'))
          break;
      if(end > str) // TODO: read escaped spaces as ordinary spaces?
        result.append(data.substr(str - start, end - str));
      str = end;
      if(*str)
        ++str; // skip closing '"'
    }
    else
    {
      const char* end = str;
      for(; *end; ++end)
        if(String::isSpace(*end))
          break;
      // TODO: read escaped spaces as ordinary spaces
      result.append(data.substr(str - start, end - str));
      str = end;
    }
  }
  return result.size();
}
