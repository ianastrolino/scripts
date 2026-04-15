#include <api.h>

extern void user_main();

int main(void) {
	/* Inicialização básica do sistema se necessário */
	
	/* Chama a tarefa principal do usuário */
	user_main();

	/* Loop de segurança */
	while (1) {
		/* Idle state */
	}

	return 0;
}

/*
 * gos/kernel/main.c
 */
