#include <api.h>
#include <arch/io.h>

void pio_set_mode(u16 port, u8 pin, u8 mode) {
	/* No AVR, DDR (Data Direction Register) é geralmente PortBase + 1 */
	u8 *reg = (u8 *) (port + 1);

	if (mode == 1) { // Output
		*reg |= (1 << pin);
	} else { // Input
		*reg &= ~(1 << pin);
	}
}

void pio_set_pin(u16 port, u8 pin, u8 val) {
	/* No AVR, PORT register é geralmente PortBase + 2 */
	u8 *reg = (u8 *) (port + 2);

	if (val) {
		*reg |= (1 << pin);
	} else {
		*reg &= ~(1 << pin);
	}
}

u8 pio_get_pin(u16 port, u8 pin) {
	/* No AVR, PIN register é geralmente no PortBase */
	u8 *reg = (u8 *) port;
	return ((*reg >> pin) & 1);
}

void j_pin_toggle(u16 port, u8 pin) {
	u8 *reg = (u8 *) (port + 2);
	*reg ^= (1 << pin);
}

/*
 * gos/arch/avr/plat/pio.c
 * 
 * Implementação da HAL para controle de pinos (PIO)
 */
