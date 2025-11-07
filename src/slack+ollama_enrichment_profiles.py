# -*- coding: utf-8 -*-
"""
Slack unfurl → followers/connections + Ollama enrichment
versión con:
- stream=False en Ollama
- timeout reducido
- recorte de texto largo
- dtypes de pandas arreglados
- salto de LLM cuando no hay info
"""

import os
import time
import re
import sys
import signal
import tempfile
import shutil
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import pandas as pd
import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


# ============================================================
# CONFIGURACIÓN — EDITA AQUÍ
# ============================================================

# CSV de entrada
CSV_PATH = Path(r"data\prueba\linkedin_unificado.csv")  # ← cámbialo a tu CSV de origen

# CSV de salida
OUT_PATH = CSV_PATH.with_name(CSV_PATH.stem + "_with_slack_counts_llm.csv")

# Slack
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = "C09NNL40TB2"

# Lotes y tiempos
BATCH_SIZE = 5
UNFURL_WAIT_SECONDS = 28
SLEEP_BETWEEN_BATCHES = 7
LIMIT_URLS = 1000

# Guardado
SAVE_PER_URL = True
BACKUP_EVERY_N_BATCHES = 50
DELETE_MESSAGES = True

# Debug
DUMP_JSON = False
LOG_DIR = Path("logs_unfurl")

# ====== LLM / OLLAMA ======
OLLAMA_ENABLED = True
OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "phi3:3.8b"
MIN_CHARS_FOR_LLM = 120   # si el unfurl tiene menos de esto, no llamamos al LLM
MAX_CHARS_FOR_LLM = 1200  # recorte para que no tarde

OLLAMA_TIMEOUT_SECONDS = 45
OLLAMA_MAX_RETRIES = 1

LLM_PROMPT = """
Analiza el siguiente texto y devuelve SOLO un JSON con las claves:
- "profesion": título profesional principal de la persona, claro y conciso, pero que conserve su significado 
  (por ejemplo: "Director de ventas", "Analista de datos", "Diseñador industrial", "Consultor", 
  "Ingeniero de software", "Abogado", "CEO", "CTO", "CFO", "COO").
  Si el texto contiene siglas de cargos directivos como CEO, CTO, CFO, COO, CIO, CMO, CHRO u otras similares, 
  respétalas y clasifícalas tal cual (por ejemplo, "CEO" → "Director general" si traduces, o mantén la sigla si aparece así en el texto).
  Si el texto describe varios roles o áreas, elige el más representativo y exprésalo de forma general, sin detalles innecesarios.
- "sector": categoría profesional general o combinada cuando sea relevante 
  (por ejemplo: "Tecnología / Salud", "Consultoría / Finanzas", "ONG / Educación", "Marketing / Publicidad"), en español.
  Si el texto se refiere a cooperación, políticas sociales o desarrollo humano, clasifica el sector como "ONG / cooperación internacional".
  Si no hay información clara sobre el sector, infiere la categoría más probable a partir del rol profesional. 
  Por ejemplo, "Business Analyst" suele pertenecer al sector "Consultoría / Finanzas".
- "es_tech": true si pertenece al ámbito tecnológico, digital, IA, software o datos; false en caso contrario.
- "contactos_linkedin": número entero. 
  Si el texto contiene expresiones como "más de 500", "500+", "more than 500" u "over 500", asigna el valor 500. 
  Si no aparece información de contactos, asigna null.
El texto puede estar en cualquier idioma, pero las respuestas deben estar en español.
NO expliques nada. NO añadas texto fuera del JSON. Responde solo con JSON válido.
""".strip()
# ============================================================


# ------------------ Normalización y utilidades ------------------
def normalize_url(u: Optional[str]) -> Optional[str]:
    if u is None or (isinstance(u, float) and pd.isna(u)):
        return None
    s = str(u).strip()
    if not s:
        return None
    if not s.startswith("http"):
        s = "https://www.linkedin.com/in/" + s.lstrip("/")
    return s.split("?", 1)[0].rstrip("/")


def add_probe_param(u: str) -> str:
    ts = str(int(time.time() * 1000))
    parsed = urlparse(u)
    q = parse_qs(parsed.query)
    q["_lc_probe"] = [ts]
    new_q = urlencode(q, doseq=True)
    return urlunparse(parsed._replace(query=new_q))


def chunked(seq: List[str], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]


# ------------------ Regex contactos/conexiones ------------------
RE_FOLLOWERS = [
    r"(\d[\d\.\, ]*\+?)\s*(followers|seguidores)\b",
    r"(followers|seguidores)\s*[:\-]?\s*(\d[\d\.\, ]*\+?)\b",
]
RE_CONNECTIONS = [
    r"(\d[\d\.\, ]*\+?)\s*(connections|conexiones|contactos)\b",
    r"(connections|conexiones|contactos)\s*[:\-]?\s*(\d[\d\.\, ]*\+?)\b",
]


def _parse_number(tok: str) -> Optional[int]:
    s = tok.strip().lower().replace(" ", "")
    mult = 1
    if s.endswith("k"):
        mult, s = 1000, s[:-1]
    elif s.endswith("m"):
        mult, s = 1_000_000, s[:-1]
    if s.endswith("+"):
        s = s[:-1]
    s = s.replace(".", "").replace(",", "")
    if not s.isdigit():
        return None
    return int(s) * mult


def extract_metrics(text: str) -> Tuple[Optional[int], Optional[int]]:
    if not text:
        return (None, None)
    t = text.lower()

    followers = None
    for pat in RE_FOLLOWERS:
        m = re.search(pat, t, re.I)
        if m:
            for g in m.groups():
                if g and re.search(r"\d", g):
                    n = _parse_number(g)
                    if n is not None:
                        followers = n
                        break
        if followers is not None:
            break

    connections = None
    for pat in RE_CONNECTIONS:
        m = re.search(pat, t, re.I)
        if m:
            for g in m.groups():
                if g and re.search(r"\d", g):
                    n = _parse_number(g)
                    if n is not None:
                        # normaliza 500+ a 500
                        if n >= 500:
                            n = 500
                        connections = n
                        break
        if connections is not None:
            break

    return (followers, connections)


# ------------------ LLM (Ollama) ------------------
def call_ollama_on_text(text: str) -> Optional[dict]:
    if not OLLAMA_ENABLED:
        return None

    # recortamos texto largo
    if len(text) > MAX_CHARS_FOR_LLM:
        text = text[:MAX_CHARS_FOR_LLM]

    payload = {
        "model": OLLAMA_MODEL,
        "stream": False,  # importante
        "messages": [
            {"role": "system", "content": LLM_PROMPT},
            {"role": "user",  "content": f"Texto: {text}"}
        ]
    }

    last_err = None
    for _ in range(1 + OLLAMA_MAX_RETRIES):
        try:
            resp = requests.post(
                OLLAMA_URL,
                json=payload,
                timeout=OLLAMA_TIMEOUT_SECONDS
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["message"]["content"]
            # nos quedamos solo con el bloque { ... }
            m = re.search(r"\{.*\}", content, re.DOTALL)
            if not m:
                return None
            json_str = m.group(0)
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                return None
        except Exception as e:
            last_err = e
            # siguiente intento
            continue

    print(f"⚠️ Ollama fallo después de reintentos: {last_err}")
    return None


def get_replies_with_retry(client: WebClient, channel: str, ts: str, tries: int = 3, pause: int = 4):
    last_exc = None
    for _ in range(tries):
        try:
            return client.conversations_replies(channel=channel, ts=ts, inclusive=True, limit=1)
        except SlackApiError as e:
            last_exc = e
            time.sleep(pause)
        except Exception as e:
            last_exc = e
            time.sleep(pause)
    raise last_exc


# ------------------ Slack ------------------
def post_batch_and_get_unfurls(client: WebClient, urls: List[str]) -> Dict[str, dict]:
    posted = [add_probe_param(u) for u in urls]
    text = "\n".join(posted)

    try:
        resp = client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=text, unfurl_links=True)
    except SlackApiError as e:
        print(f"⚠️ Error al enviar lote: {e.response.get('error')}")
        return {u: {"followers": None, "connections": None, "raw_text": None, "llm": None} for u in urls}

    ts = resp["ts"]
    time.sleep(UNFURL_WAIT_SECONDS)

    try:
        reply = get_replies_with_retry(client, SLACK_CHANNEL_ID, ts)
    except SlackApiError as e:
        print(f"⚠️ Error al leer unfurls: {e.response.get('error')}")
        return {u: {"followers": None, "connections": None, "raw_text": None, "llm": None} for u in urls}

    msg = (reply.get("messages") or [{}])[0]
    results = {u: {"followers": None, "connections": None, "raw_text": None, "llm": None} for u in urls}

    atts = msg.get("attachments", [])
    for att in atts:
        original_url = att.get("original_url") or att.get("title_link") or att.get("from_url") or ""
        text_fields = " \n ".join(str(att.get(k, "")) for k in ("text", "fallback", "title", "pretext"))
        f, c = extract_metrics(text_fields)

        key = normalize_url(original_url)
        if key:
            for u in urls:
                if normalize_url(u) == key:
                    results[u]["followers"] = results[u]["followers"] or f
                    results[u]["connections"] = results[u]["connections"] or c
                    results[u]["raw_text"] = text_fields

                    # solo llamamos al LLM si hay material
                    if text_fields and len(text_fields) >= MIN_CHARS_FOR_LLM:
                        llm_res = call_ollama_on_text(text_fields)
                    else:
                        llm_res = None
                    results[u]["llm"] = llm_res

    # fallback por posición
    if any(v["raw_text"] is None for v in results.values()) and atts:
        for idx, att in enumerate(atts):
            if idx >= len(urls):
                break
            u_guess = urls[idx]
            if results[u_guess]["raw_text"] is None:
                text_fields = " \n ".join(str(att.get(k, "")) for k in ("text", "fallback", "title", "pretext"))
                f2, c2 = extract_metrics(text_fields)
                results[u_guess]["followers"] = results[u_guess]["followers"] or f2
                results[u_guess]["connections"] = results[u_guess]["connections"] or c2
                results[u_guess]["raw_text"] = text_fields

                if text_fields and len(text_fields) >= MIN_CHARS_FOR_LLM:
                    llm_res = call_ollama_on_text(text_fields)
                else:
                    llm_res = None
                results[u_guess]["llm"] = llm_res

    if DUMP_JSON:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        dump_path = LOG_DIR / f"unfurl_{int(time.time())}.json"
        with open(dump_path, "w", encoding="utf-8") as fh:
            json.dump(msg, fh, ensure_ascii=False, indent=2)

    if DELETE_MESSAGES:
        try:
            client.chat_delete(channel=SLACK_CHANNEL_ID, ts=ts)
        except SlackApiError as e:
            print(f"⚠️ No se pudo borrar el mensaje: {e.response.get('error')}")

    filled = sum(1 for v in results.values() if (v["followers"] is not None or v["connections"] is not None))
    print(f"📦 Unfurls: attachments={len(atts)} → URLs con datos={filled}/{len(urls)}")

    return results


# ------------------ IO seguro ------------------
def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), suffix=".tmp", encoding="utf-8-sig") as tmp:
        tmp_path = Path(tmp.name)
        df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    for _ in range(3):
        try:
            tmp_path.replace(path)
            return
        except PermissionError:
            time.sleep(0.5)
    tmp_path.replace(path)


def backup_copy(src: Path) -> None:
    if not src.exists():
        return
    ts = time.strftime("%Y%m%d-%H%M%S")
    bak = src.with_suffix(f".{ts}.bak.csv")
    try:
        shutil.copy2(src, bak)
        print(f"🧰 Backup: {bak.name}")
    except Exception as e:
        print(f"⚠️ No se pudo crear backup: {e}")


# ------------------ Pipeline ------------------
def pick_url(row: pd.Series) -> Optional[str]:
    u = row.get("linkedinUrl") or row.get("salesNavigatorId") or row.get("url")
    return normalize_url(u)


def ensure_new_columns(df: pd.DataFrame) -> None:
    # creamos columnas con dtypes correctos
    if "followersSlack" not in df.columns:
        df["followersSlack"] = pd.Series(dtype="Int64")
    if "connectionsSlack" not in df.columns:
        df["connectionsSlack"] = pd.Series(dtype="Int64")

    for col in ("profesionLLM", "sectorLLM"):
        if col not in df.columns:
            df[col] = pd.Series(dtype="object")
        else:
            df[col] = df[col].astype("object")

    if "esTechLLM" not in df.columns:
        df["esTechLLM"] = pd.Series(dtype="boolean")
    else:
        if df["esTechLLM"].dtype != "boolean":
            df["esTechLLM"] = df["esTechLLM"].astype("boolean")


def build_worklist(df: pd.DataFrame, limit: int) -> List[str]:
    ensure_new_columns(df)

    urls = []
    seen = set()
    for _, row in df.iterrows():
        url = pick_url(row)
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)

        # si ya tenemos followers y conexiones, podemos saltar
        if pd.notna(row.get("followersSlack")) or pd.notna(row.get("connectionsSlack")):
            continue

        urls.append(url)

    if limit and limit > 0:
        urls = urls[:limit]
    return urls


def apply_results_to_df(df: pd.DataFrame, results: Dict[str, dict]) -> Tuple[int, int, int]:
    upd_f = upd_c = upd_llm = 0
    for i, row in df.iterrows():
        u = pick_url(row)
        if not u:
            continue
        if u not in results:
            continue
        res = results[u]
        f = res.get("followers")
        c = res.get("connections")
        raw_llm = res.get("llm")

        if f is not None and pd.isna(row.get("followersSlack")):
            df.at[i, "followersSlack"] = int(f)
            upd_f += 1
        if c is not None and pd.isna(row.get("connectionsSlack")):
            df.at[i, "connectionsSlack"] = int(c)
            upd_c += 1

        if raw_llm:
            prof = raw_llm.get("profesion")
            sect = raw_llm.get("sector")
            tech = raw_llm.get("es_tech")

            if prof and pd.isna(row.get("profesionLLM")):
                df.at[i, "profesionLLM"] = prof
                upd_llm += 1
            if sect and pd.isna(row.get("sectorLLM")):
                df.at[i, "sectorLLM"] = sect
            if tech is not None and pd.isna(row.get("esTechLLM")):
                df.at[i, "esTechLLM"] = bool(tech)

    return upd_f, upd_c, upd_llm


def main():
    print("🚀 Slack unfurl → followers/connections + Ollama enrichment\n")

    if not SLACK_BOT_TOKEN or SLACK_BOT_TOKEN.startswith("xoxb-PEGAR"):
        print("⚠️ Debes setear SLACK_BOT_TOKEN.")
        sys.exit(1)

    if OUT_PATH.exists():
        print(f"📂 Reanudando desde {OUT_PATH.name}")
        df = pd.read_csv(OUT_PATH)
    else:
        print(f"📄 Cargando base desde {CSV_PATH.name}")
        df = pd.read_csv(CSV_PATH)
        ensure_new_columns(df)
        atomic_write_csv(df, OUT_PATH)

    work = build_worklist(df, LIMIT_URLS)
    print(f"📝 URLs pendientes: {len(work)}")

    client = WebClient(token=SLACK_BOT_TOKEN)

    interrupted = {"flag": False}

    def _graceful_exit(sig, frame):
        interrupted["flag"] = True
        print("\n🛑 Interrupción capturada — guardando estado...")

    signal.signal(signal.SIGINT, _graceful_exit)
    try:
        signal.signal(signal.SIGTERM, _graceful_exit)
    except Exception:
        pass

    batch_idx = 0
    for batch in chunked(work, BATCH_SIZE):
        batch_idx += 1
        print(f"\n▶ Lote {batch_idx} — {len(batch)} enlaces")

        try:
            res = post_batch_and_get_unfurls(client, batch)
        except Exception as e:
            print(f"⚠️ Fallo en post_batch_and_get_unfurls: {e}")
            res = {u: {"followers": None, "connections": None, "raw_text": None, "llm": None} for u in batch}

        if SAVE_PER_URL:
            for u in batch:
                one = {u: res.get(u)}
                uf, uc, ul = apply_results_to_df(df, one)
                atomic_write_csv(df, OUT_PATH)
        else:
            uf, uc, ul = apply_results_to_df(df, res)
            atomic_write_csv(df, OUT_PATH)

        total_f = int(df["followersSlack"].notna().sum())
        total_c = int(df["connectionsSlack"].notna().sum())
        print(f"💾 Guardado → F+:{total_f} C+:{total_c} [{OUT_PATH.name}]")

        if BACKUP_EVERY_N_BATCHES and batch_idx % BACKUP_EVERY_N_BATCHES == 0:
            backup_copy(OUT_PATH)

        if interrupted["flag"]:
            break

        time.sleep(SLEEP_BETWEEN_BATCHES)

    atomic_write_csv(df, OUT_PATH)
    print(f"✅ Terminado. CSV actualizado: {OUT_PATH}")
    backup_copy(OUT_PATH)


if __name__ == "__main__":
    main()
