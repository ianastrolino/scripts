# GOS (Gate OS) Makefile
# Baseado no estilo JustinOS

# Configurações do Compilador
CC = avr-gcc
OBJCOPY = avr-objcopy
SIZE = avr-size

# MCU options: atmega328p, attiny85
# Uso: make MCU=atmega328p ou make MCU=attiny85
MCU ?= atmega328p

# Definições baseadas no MCU
ifeq ($(MCU),atmega328p)
	MCU_DEFINE = AVR_ATMEGA328P
else ifeq ($(MCU),attiny85)
	MCU_DEFINE = AVR_ATTINY85
else
	$(error MCU $(MCU) nao suportado. Use 'atmega328p' ou 'attiny85')
endif

# Flags de compilação
CFLAGS = -mmcu=$(MCU) -D$(MCU_DEFINE) -Iinclude -Iarch/avr/include -Os -Wall -ffreestanding
LDFLAGS = -mmcu=$(MCU) -nostdlib

# Arquivos fonte
SRCS = arch/avr/plat/pio.c \
       kernel/main.c \
       task/simple.c

# Arquivos objeto
OBJS = $(SRCS:.c=.o)

# Alvo principal
all: kernel.hex

# Linking
kernel.elf: $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^

# Gerando o binario .hex para gravação
kernel.hex: kernel.elf
	$(OBJCOPY) -O ihex $< $@
	@echo "-----------------------------------"
	@echo "Build completo para $(MCU)"
	@$(SIZE) --format=avr --mcu=$(MCU) $<
	@echo "-----------------------------------"

# Limpeza
clean:
	rm -f $(OBJS) kernel.elf kernel.hex

.PHONY: all clean
