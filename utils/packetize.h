#pragma once
#include "global.h"

using namespace std;

class UnstructuredBuffer
{
private:
    char * _buf;
    uint32_t _pos;
    string _chars;
public:
    UnstructuredBuffer() {
        _buf = NULL;
        _pos = 0;
    }
    UnstructuredBuffer(char * buf) {
        _buf = buf;
        _pos = 0;
    }

    char * data() { return _buf ? _buf : _chars.data(); }
    uint32_t size();

    template<class T> void put(T * data);
    template<class T> void get(T * data);

    template<class T> void put_front(T * data);
    template<class T> void put_at(T * data, uint32_t pos);

    void put(char * data, uint32_t size);
    void put(void * data, unsigned int size);
    void get(char * &data, uint32_t size);
    void get(void * &data, unsigned int size);
    uint32_t get_offset() { return _pos; }
    void     set_offset(uint32_t offset) { _pos = offset; }
    template<class T> void set(uint32_t pos, T *data);
};

template<class T>
void UnstructuredBuffer::put(T * data)
{
    if (_buf) {
        memcpy(_buf + _pos, data, sizeof(T));
        _pos += sizeof(T);
    } else {
        _chars.append((char *)data, sizeof(T));
    }
}

template<class T>
void UnstructuredBuffer::get(T * data)
{
    memcpy(data, _buf + _pos, sizeof(T));
    _pos += sizeof(T);
}

template<class T>
void UnstructuredBuffer::set(uint32_t pos, T *data)
{
    if (_buf) {
        memcpy(_buf + pos, data, sizeof(T));
    } else {
        memcpy(_chars.data() + pos, data, sizeof(T));
    }
}

template<class T>
void UnstructuredBuffer::put_front(T * data)
{
    put_at(data, 0);
}

template<class T>
void UnstructuredBuffer::put_at(T * data, uint32_t pos)
{
    assert(_buf == NULL);
    _chars.insert(pos, (char *)data, sizeof(T));
}
