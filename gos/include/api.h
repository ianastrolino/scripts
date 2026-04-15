#ifndef INCLUDE_API_H_
#define INCLUDE_API_H_

#include "types.h"

/* HAL abstractions */
void pio_set_mode(u16 port, u8 pin, u8 mode);
void pio_set_pin(u16 port, u8 pin, u8 val);
u8 pio_get_pin(u16 port, u8 pin);
void j_pin_toggle(u16 port, u8 pin);

#endif /* INCLUDE_API_H_ */
/*
 * gos/include/api.h
 */
