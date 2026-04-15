#!/usr/bin/env python3
"""
criar_usuario.py — Gera o hash de senha para adicionar no USERS_CONFIG do Railway.

Uso:
  python criar_usuario.py

O script pede o e-mail, nome e senha interativamente e imprime
o trecho JSON pronto para colar no USERS_CONFIG.

Exemplo de saida:
  "usuario@astrovistorias.com.br": {
    "password_hash": "a1b2c3...:d4e5f6...",
    "unit": "moema",
    "master": false,
    "name": "Nome do Usuario"
  }
"""
from __future__ import annotations

import getpass
import hashlib
import json
import secrets
import sys


def _hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 200_000)
    return f"{salt}:{dk.hex()}"


def main() -> None:
    print("=== Criar usuario para o Frente de Caixa Astrovistorias ===\n")

    email = input("E-mail (@astrovistorias.com.br): ").strip().lower()
    if not email:
        print("E-mail nao pode ser vazio.", file=sys.stderr)
        sys.exit(1)
    if not email.endswith("@astrovistorias.com.br"):
        print("Atencao: e-mail nao termina com @astrovistorias.com.br.", file=sys.stderr)

    name = input("Nome do usuario: ").strip()

    units_exemplos = "moema, barueri, mooca, sao_miguel, itu, indianopolis, sorocaba"
    unit = input(f"Unidade ({units_exemplos}) — ou deixe vazio para master: ").strip().lower()

    is_master = (unit == "" or unit == "master")
    if is_master:
        unit_value = None
        master_value = True
    else:
        unit_value = unit
        master_value = False

    pw1 = getpass.getpass("Senha: ")
    pw2 = getpass.getpass("Confirme a senha: ")
    if pw1 != pw2:
        print("As senhas nao coincidem.", file=sys.stderr)
        sys.exit(1)
    if len(pw1) < 8:
        print("Senha muito curta (minimo 8 caracteres).", file=sys.stderr)
        sys.exit(1)

    pw_hash = _hash_password(pw1)

    user_entry: dict = {
        "password_hash": pw_hash,
        "master": master_value,
        "name": name,
    }
    if unit_value:
        user_entry["unit"] = unit_value

    print("\n" + "=" * 60)
    print("Cole o trecho abaixo no USERS_CONFIG (dentro do objeto JSON):")
    print("=" * 60)
    print(f'  "{email}": {json.dumps(user_entry, ensure_ascii=False, indent=4)}')
    print("=" * 60)
    print("\nExemplo de USERS_CONFIG completo:")
    full_example = {email: user_entry}
    print(json.dumps(full_example, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
