
#pragma once

#include <nstd/String.h>
#include <nstd/List.h>

class Word
{
public:
  static size_t split(const String& data, List<String>& result);
};
