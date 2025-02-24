#pragma once

#include <stdint.h>
#include <mm_malloc.h>
#include "pthread.h"

#define BACKOFF \
	__asm__ ( "pause;" );  __asm__ ( "pause;" ); __asm__ ( "pause;" ); __asm__ ( "pause;" ); __asm__ ( "pause;" ); \
	__asm__ ( "pause;" );  __asm__ ( "pause;" ); __asm__ ( "pause;" ); __asm__ ( "pause;" ); __asm__ ( "pause;" ); \
	__asm__ ( "pause;" );  __asm__ ( "pause;" ); __asm__ ( "pause;" ); __asm__ ( "pause;" ); __asm__ ( "pause;" ); \
	__asm__ ( "pause;" );  __asm__ ( "pause;" ); __asm__ ( "pause;" ); __asm__ ( "pause;" ); __asm__ ( "pause;" ); \
	__asm__ ( "pause;" );  __asm__ ( "pause;" ); __asm__ ( "pause;" ); __asm__ ( "pause;" ); __asm__ ( "pause;" ); \

template<int cap>
class RingBuffer
{
public:
    RingBuffer() 
	{
		_head = 0;
		_size = 0;
		pthread_spin_init(&_latch, 0);
	}

	template<typename U>
    void block_push(U const &value) 
	{
		while (_size >= cap) 
			BACKOFF;

		pthread_spin_lock(&_latch);
		while (_size >= cap) {
			pthread_spin_unlock(&_latch);
			BACKOFF;
			pthread_spin_lock(&_latch);
		}
		uint32_t idx = (_head + _size) % cap;
		_buffer[idx] = (uintptr_t) value;
		_size = _size + 1;
		pthread_spin_unlock(&_latch);
	}

	template<typename U>
    bool push(U const &value) 
	{
		if (_size >= cap) 
			return false;

		pthread_spin_lock(&_latch);
		if (_size >= cap) {
			pthread_spin_unlock(&_latch);
			return false;
		}
		uint32_t idx = (_head + _size) % cap;
		_buffer[idx] = (uintptr_t) value;
		_size = _size + 1;
		pthread_spin_unlock(&_latch);
		return true;
	}

	template<typename U>
    bool pop(U &value)
	{
		if (_size == 0)
			return false;
		
		pthread_spin_lock(&_latch);
		if (_size == 0) {
			pthread_spin_unlock(&_latch);
			return false;
		}

		value = (U)_buffer[_head];
		_head = (_head + 1) % cap;
		_size = _size - 1;
		pthread_spin_unlock(&_latch);
		return true;
	}
	bool empty() {
		return _size == 0;
	}
protected:
    uint32_t volatile _head;
    uint32_t volatile _size;
	pthread_spinlock_t _latch;
	uintptr_t _buffer[cap];
};