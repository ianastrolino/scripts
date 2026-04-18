"""
Testes de integração para o fluxo de fechamento: preview, send e clear-imported.

Estratégia:
- api/preview  → sem chamadas externas; testável direto
- api/send     → TinyImporter.create_accounts_receivable mockado
- api/clear-imported → manipulação de imported.json
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402
from tiny_import import load_state, save_state  # noqa: E402

UNIT = "testunit"
FAKE_USER = {"email": "op@astrovistorias.com.br", "name": "Operador", "unit": UNIT, "master": False}

FULL_UNITS = {
    UNIT: {
        "nome": "Test Unit",
        "master_pin": "1234",
        "forma_recebimento_ids": {"AV": 101, "dinheiro": 201, "pix": 301, "debito": 401, "credito": 501},
        "categoria_ids": {"LAUDO DE TRANSFERENCIA": 999, "VISTORIA CAUTELAR": 888},
        "include_forma_recebimento": True,
        "numero_documento_prefix": "PLANILHA",
        "aliases": {"servico": {}, "fp": {}, "cliente": {}},
    }
}

# Record mínimo válido para preview/send
BASE_RECORD = {
    "id": "r1",
    "data": "2026-04-17",
    "modelo": "GOL",
    "placa": "ABC1234",
    "cliente": "CLIENTE TESTE",
    "servico": "LAUDO DE TRANSFERENCIA",
    "fp": "AV",
    "preco": "150.00",
    "avPagamento": "AV",
    "origemArquivo": "planilha.xlsx",
    "linhaOrigem": 2,
}


@pytest.fixture
def client(tmp_path):
    server.DATA_DIR = tmp_path
    server.UNITS = FULL_UNITS
    server.app.config["TESTING"] = True
    with patch.object(server, "_current_user", return_value=FAKE_USER):
        with server.app.test_client() as c:
            yield c


def _preview(client, records):
    return client.post(f"/u/{UNIT}/api/preview", json={"records": records})


def _send(client, records):
    return client.post(f"/u/{UNIT}/api/send", json={"records": records})


# ══════════════════════════════════════════════════════════════════════════════
# preview
# ══════════════════════════════════════════════════════════════════════════════

class TestPreview:
    def test_ok_campos_basicos(self, client):
        r = _preview(client, [BASE_RECORD])
        assert r.status_code == 200
        body = r.get_json()
        assert body["success"] is True
        p = body["previews"][0]
        assert p["chave"] == "r1"
        assert p["cliente"] == "CLIENTE TESTE"
        assert p["valor"] == 150.0
        assert p["servico"] == "LAUDO DE TRANSFERENCIA"

    def test_resumo_contagem(self, client):
        r2 = {**BASE_RECORD, "id": "r2"}
        body = _preview(client, [BASE_RECORD, r2]).get_json()
        assert body["resumo"]["total"] == 2
        assert body["resumo"]["novos"] == 2
        assert body["resumo"]["duplicatas"] == 0

    def test_ja_enviado_detectado(self, client, tmp_path):
        state_path = tmp_path / UNIT / "imported.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        save_state(state_path, {"imported": {"r1": {"enviado_em": "2026-04-17T10:00:00"}}})
        body = _preview(client, [BASE_RECORD]).get_json()
        assert body["previews"][0]["jaEnviado"] is True
        assert body["resumo"]["duplicatas"] == 1
        assert body["resumo"]["novos"] == 0

    def test_forma_recebimento_mapeada(self, client):
        body = _preview(client, [BASE_RECORD]).get_json()
        forma = body["previews"][0]["formaRecebimento"]
        assert "101" in forma  # ID de AV conforme UNITS_CONFIG

    def test_categoria_resolvida(self, client):
        body = _preview(client, [BASE_RECORD]).get_json()
        payload = body["previews"][0]["payload"]
        assert payload.get("categoria", {}).get("id") == 999

    def test_lista_vazia(self, client):
        body = _preview(client, []).get_json()
        assert body["previews"] == []
        assert body["resumo"]["total"] == 0

    def test_ignora_fp_nao_av_no_preview(self, client):
        rec = {**BASE_RECORD, "fp": "PIX", "avPagamento": ""}
        body = _preview(client, [rec]).get_json()
        # Registros não-AV ainda aparecem no preview (são enviados ao Tiny normalmente)
        assert len(body["previews"]) == 1


# ══════════════════════════════════════════════════════════════════════════════
# send
# ══════════════════════════════════════════════════════════════════════════════

class TestSend:
    def _mock_importer(self, return_value=None, side_effect=None):
        mock = MagicMock()
        if side_effect:
            mock.create_accounts_receivable.side_effect = side_effect
        else:
            mock.create_accounts_receivable.return_value = return_value or {"id": 42}
        return mock

    def test_enviado_com_sucesso(self, client, tmp_path):
        mock = self._mock_importer()
        with patch("server.TinyImporter", return_value=mock):
            body = _send(client, [BASE_RECORD]).get_json()
        assert body["success"] is True
        assert len(body["summary"]["enviados"]) == 1
        assert body["summary"]["enviados"][0]["chave"] == "r1"
        assert body["summary"]["falhas"] == []

    def test_enviado_persiste_em_imported_json(self, client, tmp_path):
        mock = self._mock_importer()
        with patch("server.TinyImporter", return_value=mock):
            _send(client, [BASE_RECORD])
        state = load_state(tmp_path / UNIT / "imported.json")
        assert "r1" in state["imported"]

    def test_dedup_local_pula_sem_chamar_tiny(self, client, tmp_path):
        state_path = tmp_path / UNIT / "imported.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        save_state(state_path, {"imported": {"r1": {"enviado_em": "2026-04-17T09:00:00"}}})
        mock = self._mock_importer()
        with patch("server.TinyImporter", return_value=mock):
            body = _send(client, [BASE_RECORD]).get_json()
        mock.create_accounts_receivable.assert_not_called()
        assert body["summary"]["pulados"][0]["motivo"] == "ja importado"

    def test_erro_generico_vai_para_falhas(self, client):
        mock = self._mock_importer(side_effect=Exception("timeout na API"))
        with patch("server.TinyImporter", return_value=mock):
            body = _send(client, [BASE_RECORD]).get_json()
        assert body["success"] is True  # rota não explode
        assert len(body["summary"]["falhas"]) == 1
        assert "timeout na API" in body["summary"]["falhas"][0]["erro"]

    def test_doc_ja_existia_no_tiny_vai_para_pulados(self, client):
        exc = Exception("numeroDocumento ja cadastrado")
        with patch("server.TinyImporter", return_value=self._mock_importer(side_effect=exc)):
            with patch("server._is_doc_already_registered", return_value=True):
                body = _send(client, [BASE_RECORD]).get_json()
        assert body["summary"]["pulados"][0]["motivo"] == "ja existia no Tiny"

    def test_lote_misto_enviado_pulado_falha(self, client, tmp_path):
        r_ok    = {**BASE_RECORD, "id": "ok1"}
        r_dup   = {**BASE_RECORD, "id": "dup1"}
        r_falha = {**BASE_RECORD, "id": "err1"}

        # pré-importa dup1
        state_path = tmp_path / UNIT / "imported.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        save_state(state_path, {"imported": {"dup1": {}}})

        call_count = 0
        def _side_effect(rec):
            nonlocal call_count
            call_count += 1
            if rec.chave_deduplicacao == "err1":
                raise Exception("erro pontual")
            return {"id": 99}

        with patch("server.TinyImporter", return_value=self._mock_importer(side_effect=_side_effect)):
            body = _send(client, [r_ok, r_dup, r_falha]).get_json()

        assert len(body["summary"]["enviados"]) == 1
        assert len(body["summary"]["pulados"]) == 1
        assert len(body["summary"]["falhas"]) == 1


# ══════════════════════════════════════════════════════════════════════════════
# clear-imported
# ══════════════════════════════════════════════════════════════════════════════

class TestClearImported:
    def test_limpa_registros(self, client, tmp_path):
        state_path = tmp_path / UNIT / "imported.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        save_state(state_path, {"imported": {"r1": {}, "r2": {}}})
        body = client.post(f"/u/{UNIT}/api/clear-imported").get_json()
        assert body["success"] is True
        assert "2" in body["message"]
        state = load_state(state_path)
        assert state["imported"] == {}

    def test_funciona_sem_arquivo_existente(self, client):
        body = client.post(f"/u/{UNIT}/api/clear-imported").get_json()
        assert body["success"] is True
        assert "0" in body["message"]
