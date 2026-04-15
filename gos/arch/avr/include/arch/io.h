#ifndef ARCH_AVR_INCLUDE_ARCH_IO_H_
#define ARCH_AVR_INCLUDE_ARCH_IO_H_

#ifdef AVR_ATMEGA328P
#  include "atmega328p/io.h"
#elif defined(AVR_ATTINY85)
#  include "attiny85/io.h"
#else
#  error "AVR processor not specified or not supported"
#endif

#include <types.h>

#endif /* ARCH_AVR_INCLUDE_ARCH_IO_H_ */
/*
 * gos/arch/avr/include/arch/io.h
 */
