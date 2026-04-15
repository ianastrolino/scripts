#include <api.h>
#include <arch/io.h>

void user_main() {
#ifdef AVR_ATMEGA328P
	/* Arduino Uno Built-in LED (Digital 13) is PB5 */
	u8 pin = PIO_PIN5;
	u16 port = PIOB_BASE;
#elif defined(AVR_ATTINY85)
	/* ATtiny85 common pin for led */
	u8 pin = PIO_PIN1;
	port = PIOB_BASE;
#else
	u8 pin = PIO_PIN0;
	u16 port = PIOA_BASE;
#endif

	/* Configura o pino como saída */
	pio_set_mode(port, pin, 1);

	while (1) {
		/* Inverte o estado do pino */
		j_pin_toggle(port, pin);

		/* Delay extremamente simples (bloqueante) */
		for (volatile long i = 0; i < 200000; i++) {
			__asm__("nop");
		}
	}
}

/*
 * gos/task/simple.c
 * 
 * Tarefa Simples: Piscar LED (Blink)
 */
