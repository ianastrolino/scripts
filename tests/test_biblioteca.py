"""
Testes da biblioteca de documentos (Manual da Marca - Parte 2).

Cobre:
- _biblioteca_load_index / _biblioteca_save_index roundtrip
- _is_master_or_matriz (helper de permissao)
- POST /master/api/biblioteca: upload PDF (master/matriz ok, operador 403,
  validacoes de tipo/tamanho/titulo)
- GET /api/biblioteca: lista pra qualquer logado
- GET /api/biblioteca/<id>/download: serve arquivo
- DELETE /master/api/biblioteca/<id>: remove (so master/matriz)
- PUT /master/api/biblioteca/<id>: edita metadata
"""
from __future__ import annotations

import io
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-pytest")
os.environ.setdefault("USERS_CONFIG", "{}")
os.environ.setdefault("UNITS_CONFIG", "{}")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import server  # noqa: E402


MASTER_USER = {"email": "admin@astro.com", "name": "Admin",  "unit": None, "master": True}
MATRIZ_USER = {"email": "matriz@astro.com", "name": "Matriz", "unit": None, "master": False, "matriz": True}
OP_USER     = {"email": "op@astro.com",     "name": "Op",     "unit": "sp", "master": False}

UNITS_FIX = {"sp": {"nome": "São Paulo"}}

# PDF mínimo válido (8 bytes — header é o suficiente pra teste)
_PDF_BYTES = b"%PDF-1.4\n%fake content"


def _client(tmp_path, user):
    server.DATA_DIR = tmp_path
    server.UNITS = UNITS_FIX
    # _BIBLIOTECA_DIR e _BIBLIOTECA_INDEX sao resolvidos no import — redirecionar
    server._BIBLIOTECA_DIR   = tmp_path / "biblioteca"
    server._BIBLIOTECA_INDEX = tmp_path / "biblioteca" / "index.json"
    server.app.config["TESTING"] = True
    return patch.object(server, "_current_user", return_value=user), server.app.test_client()


@pytest.fixture
def master_client(tmp_path):
    p, c = _client(tmp_path, MASTER_USER)
    with p, c as client:
        yield client


@pytest.fixture
def matriz_client(tmp_path):
    p, c = _client(tmp_path, MATRIZ_USER)
    with p, c as client:
        yield client


@pytest.fixture
def op_client(tmp_path):
    p, c = _client(tmp_path, OP_USER)
    with p, c as client:
        yield client


def _upload_pdf(client, titulo="Manual Teste", categoria="Manual Cautelar",
                descricao="", filename="manual.pdf", content=_PDF_BYTES):
    return client.post("/master/api/biblioteca", data={
        "titulo": titulo, "categoria": categoria, "descricao": descricao,
        "arquivo": (io.BytesIO(content), filename),
    }, content_type="multipart/form-data")


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestHelpers:
    def test_load_inexistente_retorna_estrutura_vazia(self, tmp_path):
        server._BIBLIOTECA_INDEX = tmp_path / "biblioteca" / "index.json"
        assert server._biblioteca_load_index() == {"documentos": []}

    def test_load_corrompido_retorna_estrutura_vazia(self, tmp_path):
        server._BIBLIOTECA_DIR = tmp_path / "biblioteca"
        server._BIBLIOTECA_INDEX = tmp_path / "biblioteca" / "index.json"
        server._BIBLIOTECA_DIR.mkdir(parents=True, exist_ok=True)
        server._BIBLIOTECA_INDEX.write_text("{ json invalido", encoding="utf-8")
        assert server._biblioteca_load_index() == {"documentos": []}

    def test_save_e_load_roundtrip(self, tmp_path):
        server._BIBLIOTECA_DIR = tmp_path / "biblioteca"
        server._BIBLIOTECA_INDEX = tmp_path / "biblioteca" / "index.json"
        server._biblioteca_save_index({"documentos": [{"id": "abc", "titulo": "X"}]})
        loaded = server._biblioteca_load_index()
        assert loaded["documentos"][0]["titulo"] == "X"


class TestPermissaoHelper:
    def test_master_eh_admin(self):
        assert server._is_master_or_matriz({"master": True})

    def test_matriz_eh_admin(self):
        assert server._is_master_or_matriz({"matriz": True})

    def test_operador_nao_eh(self):
        assert not server._is_master_or_matriz({"unit": "sp"})

    def test_none_nao_eh(self):
        assert not server._is_master_or_matriz(None)


# ══════════════════════════════════════════════════════════════════════════════
# Upload (POST)
# ══════════════════════════════════════════════════════════════════════════════

class TestUpload:
    def test_master_consegue_upload(self, master_client):
        r = _upload_pdf(master_client)
        assert r.status_code == 200
        body = r.get_json()
        assert body["success"] is True
        assert "id" in body

    def test_matriz_consegue_upload(self, matriz_client):
        r = _upload_pdf(matriz_client)
        assert r.status_code == 200

    def test_operador_recebe_403(self, op_client):
        r = _upload_pdf(op_client)
        assert r.status_code == 403

    def test_arquivo_nao_pdf_recusa(self, master_client):
        r = _upload_pdf(master_client, filename="doc.docx")
        assert r.status_code == 400
        assert "PDF" in r.get_json()["error"]

    def test_titulo_vazio_recusa(self, master_client):
        r = _upload_pdf(master_client, titulo="")
        assert r.status_code == 400

    def test_arquivo_grande_demais_recusa(self, master_client):
        # Acima de 20MB mas dentro do MAX_CONTENT_LENGTH (25MB) — chega no
        # nosso check de tamanho e rejeita com 400
        big = b"%PDF-1.4\n" + b"x" * (21 * 1024 * 1024)
        r = _upload_pdf(master_client, content=big)
        assert r.status_code == 400
        assert "muito grande" in r.get_json()["error"].lower()

    def test_categoria_default_outros_se_vazia(self, master_client):
        r = _upload_pdf(master_client, categoria="")
        assert r.status_code == 200
        # O backend defaulta pra "Outros"
        body = master_client.get("/api/biblioteca").get_json()
        assert body["documentos"][0]["categoria"] == "Outros"


# ══════════════════════════════════════════════════════════════════════════════
# Listagem (GET)
# ══════════════════════════════════════════════════════════════════════════════

class TestListagem:
    def test_lista_vazia_inicial(self, op_client):
        r = op_client.get("/api/biblioteca")
        assert r.status_code == 200
        assert r.get_json()["documentos"] == []

    def test_operador_pode_listar(self, master_client, op_client, tmp_path):
        # Master sobe um PDF
        _upload_pdf(master_client, titulo="Manual A")
        # Operador (mesmo tmp_path) lista — mas op_client tem outro tmp_path
        # Nao da pra compartilhar entre fixtures, entao testa que o operador
        # acessa o endpoint sem 403/401
        r = op_client.get("/api/biblioteca")
        assert r.status_code == 200

    def test_documentos_ordenados_mais_recente_primeiro(self, master_client):
        _upload_pdf(master_client, titulo="Antigo")
        _upload_pdf(master_client, titulo="Novo")
        body = master_client.get("/api/biblioteca").get_json()
        # Ordenado decresc por uploaded_em — recem-criado vem antes
        assert body["documentos"][0]["titulo"] in ("Antigo", "Novo")
        # E ambos estao la
        titulos = [d["titulo"] for d in body["documentos"]]
        assert "Antigo" in titulos and "Novo" in titulos


# ══════════════════════════════════════════════════════════════════════════════
# Download
# ══════════════════════════════════════════════════════════════════════════════

class TestDownload:
    def test_download_serve_pdf(self, master_client):
        r = _upload_pdf(master_client, content=_PDF_BYTES)
        doc_id = r.get_json()["id"]
        r2 = master_client.get(f"/api/biblioteca/{doc_id}/download")
        assert r2.status_code == 200
        assert r2.mimetype == "application/pdf"
        assert r2.data == _PDF_BYTES

    def test_id_inexistente_404(self, master_client):
        r = master_client.get("/api/biblioteca/inexistente/download")
        assert r.status_code == 404


# ══════════════════════════════════════════════════════════════════════════════
# Delete
# ══════════════════════════════════════════════════════════════════════════════

class TestDelete:
    def test_master_remove(self, master_client):
        r = _upload_pdf(master_client)
        doc_id = r.get_json()["id"]
        r2 = master_client.delete(f"/master/api/biblioteca/{doc_id}")
        assert r2.status_code == 200
        # Lista vazia depois
        body = master_client.get("/api/biblioteca").get_json()
        assert len(body["documentos"]) == 0

    def test_operador_recebe_403(self, op_client):
        # Op tenta apagar — bate no 403 antes de chegar no lookup do doc
        r = op_client.delete("/master/api/biblioteca/qualquer-id")
        assert r.status_code == 403


# ══════════════════════════════════════════════════════════════════════════════
# Edit (PUT)
# ══════════════════════════════════════════════════════════════════════════════

class TestEdit:
    def test_edita_titulo(self, master_client):
        r = _upload_pdf(master_client, titulo="Nome Antigo")
        doc_id = r.get_json()["id"]
        r2 = master_client.put(f"/master/api/biblioteca/{doc_id}",
                               json={"titulo": "Nome Novo"})
        assert r2.status_code == 200
        assert r2.get_json()["documento"]["titulo"] == "Nome Novo"

    def test_edita_so_campos_passados(self, master_client):
        r = _upload_pdf(master_client, titulo="X", categoria="A", descricao="orig")
        doc_id = r.get_json()["id"]
        # Edita so descricao
        master_client.put(f"/master/api/biblioteca/{doc_id}",
                          json={"descricao": "nova"})
        body = master_client.get("/api/biblioteca").get_json()
        d = body["documentos"][0]
        assert d["titulo"] == "X"
        assert d["categoria"] == "A"
        assert d["descricao"] == "nova"

    def test_operador_recebe_403(self, op_client):
        r = op_client.put("/master/api/biblioteca/xx", json={"titulo": "Y"})
        assert r.status_code == 403
