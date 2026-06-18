"""
Testes da fila Omie (omie_queue) em caixa_db.

Cobre:
- enqueue_omie: enfileirar, dedup, skip
- dequeue_omie: pegar pendentes, marcar processing
- complete/fail: transições de status
- omie_queue_status: contagem por status
- omie_queue_clear_done: limpeza
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from caixa_db import (
    enqueue_omie,
    dequeue_omie,
    complete_omie_queue,
    fail_omie_queue,
    omie_queue_status,
    omie_queue_clear_done,
)


class TestEnqueue:
    def test_enfileira_registros(self, tmp_path):
        records = [
            {"id": "chave1", "cliente": "HLM", "data": "2026-06-11", "servico": "X", "fp": "AV", "preco": 100},
            {"id": "chave2", "cliente": "TURBI", "data": "2026-06-11", "servico": "X", "fp": "FA", "preco": 90},
        ]
        result = enqueue_omie("ind", tmp_path, records, "2026-06-11T10:00:00")
        assert result["enqueued"] == 2
        assert result["skipped"] == 0

    def test_dedup_ignora_repetido(self, tmp_path):
        records = [{"id": "chave1", "cliente": "A", "data": "2026-06-11", "servico": "X", "fp": "AV", "preco": 100}]
        enqueue_omie("ind", tmp_path, records, "2026-06-11T10:00:00")
        result = enqueue_omie("ind", tmp_path, records, "2026-06-11T10:01:00")
        assert result["enqueued"] == 0
        assert result["skipped"] == 1

    def test_sem_chave_ignora(self, tmp_path):
        records = [{"cliente": "X", "data": "2026-06-11"}]
        result = enqueue_omie("ind", tmp_path, records, "2026-06-11T10:00:00")
        assert result["enqueued"] == 0


class TestDequeue:
    def test_pega_pendentes(self, tmp_path):
        records = [
            {"id": "c1", "cliente": "A", "data": "2026-06-11", "servico": "X", "fp": "AV", "preco": 100},
            {"id": "c2", "cliente": "B", "data": "2026-06-11", "servico": "X", "fp": "FA", "preco": 90},
        ]
        enqueue_omie("ind", tmp_path, records, "2026-06-11T10:00:00")
        items = dequeue_omie("ind", tmp_path, limit=1)
        assert len(items) == 1
        assert items[0]["chave"] == "c1"
        assert items[0]["record"]["cliente"] == "A"

    def test_dequeue_muda_status_pra_processing(self, tmp_path):
        records = [{"id": "c1", "cliente": "A", "data": "2026-06-11", "servico": "X", "fp": "AV", "preco": 100}]
        enqueue_omie("ind", tmp_path, records, "2026-06-11T10:00:00")
        dequeue_omie("ind", tmp_path, limit=1)
        qs = omie_queue_status("ind", tmp_path)
        assert qs["processing"] == 1
        assert qs["pending"] == 0

    def test_dequeue_vazio_retorna_lista_vazia(self, tmp_path):
        items = dequeue_omie("ind", tmp_path, limit=1)
        assert items == []


class TestComplete:
    def test_marca_done(self, tmp_path):
        records = [{"id": "c1", "cliente": "A", "data": "2026-06-11", "servico": "X", "fp": "AV", "preco": 100}]
        enqueue_omie("ind", tmp_path, records, "2026-06-11T10:00:00")
        items = dequeue_omie("ind", tmp_path, limit=1)
        complete_omie_queue(tmp_path, items[0]["queue_id"], "2026-06-11T10:01:00")
        qs = omie_queue_status("ind", tmp_path)
        assert qs["done"] == 1
        assert qs["pending"] == 0


class TestFail:
    def test_volta_pra_pending_apos_1a_falha(self, tmp_path):
        records = [{"id": "c1", "cliente": "A", "data": "2026-06-11", "servico": "X", "fp": "AV", "preco": 100}]
        enqueue_omie("ind", tmp_path, records, "2026-06-11T10:00:00")
        items = dequeue_omie("ind", tmp_path, limit=1)
        fail_omie_queue(tmp_path, items[0]["queue_id"], "erro x", "2026-06-11T10:01:00")
        qs = omie_queue_status("ind", tmp_path)
        assert qs["pending"] == 1
        assert qs["failed"] == 0

    def test_failed_apos_3_tentativas(self, tmp_path):
        records = [{"id": "c1", "cliente": "A", "data": "2026-06-11", "servico": "X", "fp": "AV", "preco": 100}]
        enqueue_omie("ind", tmp_path, records, "2026-06-11T10:00:00")
        for i in range(3):
            items = dequeue_omie("ind", tmp_path, limit=1)
            assert len(items) == 1
            fail_omie_queue(tmp_path, items[0]["queue_id"], f"erro {i}", "2026-06-11T10:0{i}:00")
        qs = omie_queue_status("ind", tmp_path)
        assert qs["failed"] == 1
        assert qs["pending"] == 0


class TestStatus:
    def test_status_vazio(self, tmp_path):
        qs = omie_queue_status("ind", tmp_path)
        assert qs["total"] == 0
        assert qs["pending"] == 0

    def test_status_misto(self, tmp_path):
        records = [
            {"id": "c1", "cliente": "A", "data": "2026-06-11", "servico": "X", "fp": "AV", "preco": 100},
            {"id": "c2", "cliente": "B", "data": "2026-06-11", "servico": "X", "fp": "FA", "preco": 90},
            {"id": "c3", "cliente": "C", "data": "2026-06-11", "servico": "X", "fp": "AV", "preco": 80},
        ]
        enqueue_omie("ind", tmp_path, records, "2026-06-11T10:00:00")
        items = dequeue_omie("ind", tmp_path, limit=1)
        complete_omie_queue(tmp_path, items[0]["queue_id"], "2026-06-11T10:01:00")
        qs = omie_queue_status("ind", tmp_path)
        assert qs["done"] == 1
        assert qs["pending"] == 2
        assert qs["total"] == 3


class TestClearDone:
    def test_remove_apenas_concluidos(self, tmp_path):
        records = [
            {"id": "c1", "cliente": "A", "data": "2026-06-11", "servico": "X", "fp": "AV", "preco": 100},
            {"id": "c2", "cliente": "B", "data": "2026-06-11", "servico": "X", "fp": "FA", "preco": 90},
        ]
        enqueue_omie("ind", tmp_path, records, "2026-06-11T10:00:00")
        items = dequeue_omie("ind", tmp_path, limit=1)
        complete_omie_queue(tmp_path, items[0]["queue_id"], "2026-06-11T10:01:00")
        removed = omie_queue_clear_done("ind", tmp_path)
        assert removed == 1
        qs = omie_queue_status("ind", tmp_path)
        assert qs["total"] == 1
        assert qs["pending"] == 1
