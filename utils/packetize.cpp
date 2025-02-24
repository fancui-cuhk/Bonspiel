#include "packetize.h"

void
UnstructuredBuffer::put(char * data, uint32_t size)
{
    if (_buf) {
        memcpy(_buf + _pos, data, size);
        _pos += size;
    } else {
        _chars.append(data, size);
    }
}

void
UnstructuredBuffer::put(void * data, unsigned int size)
{
    if (_buf) {
        memcpy(_buf + _pos, data, size);
        _pos += size;
    } else {
        _chars.append((char*)data, (size_t)size);
    }
}

void
UnstructuredBuffer::get(char * &data, uint32_t size)
{
    assert(_buf);
    data = _buf + _pos;
    _pos += size;
}

void
UnstructuredBuffer::get(void * &data, unsigned int size)
{
    assert(_buf);
    data = _buf + _pos;
    _pos += size;
}

uint32_t
UnstructuredBuffer::size()
{
    return _buf ? _pos : _chars.size();
}
