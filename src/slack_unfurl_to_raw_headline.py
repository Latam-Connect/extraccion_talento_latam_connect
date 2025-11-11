import os
import time
from pathlib import Path
from typing import List, Dict, Iterable, Any, Optional
import urllib.parse as up

import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────
# CARGA .env (intenta 1 nivel arriba por si ejecutas desde src/)
# ─────────────────────────────────────────────────────────────
here = Path(__file__).resolve()
project_root = here.parents[1]  # D:\LATAM CONNECT\talento_latam_connect_V0.0\extraccion_talento_latam_connect
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
# ruta del csv original con las urls de linkedin
CSV_PATH = project_root / "data" / "linkedin_unificado_valencia_country_followers.csv"
# ruta del csv de salida que se va rellenando
OUT_PATH = project_root / "data" / "linkedin_unificado_valencia_country_followers_with_raw_headline.csv"

SLACK_BOT_TOKEN = os.getenv("SLACK_ACCESS_TOKEN") or os.getenv("SLACK_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID", "")

# cuántos links por lote
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "3"))

# segundos que esperamos a que Slack haga el unfurl
UNFURL_WAIT_SECONDS = int(os.getenv("UNFURL_WAIT_SECONDS", "20"))

# dormir entre lotes para no abusar
SLEEP_BETWEEN_BATCHES = float(os.getenv("SLEEP_BETWEEN_BATCHES", "1.2"))

# si queremos borrar el mensaje después
DELETE_MESSAGES = os.getenv("DELETE_MESSAGES", "true").lower() == "true"

# limitar urls para pruebas
LIMIT_URLS = int(os.getenv("LIMIT_URLS", "0"))  # 0 = sin límite


# ─────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────
def chunked(iterable: Iterable[Any], n: int) -> Iterable[List[Any]]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == n:
            yield batch
            batch = []
    if batch:
        yield batch


def normalize_url(url: Optional[str]) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    url = url.strip()
    if not url:
        return None
    parsed = up.urlparse(url)
    cleaned = parsed._replace(query="", fragment="")
    return cleaned.geturl()


def add_probe_param(url: str) -> str:
    """
    Usamos el patrón que vimos que sí funciona:
    ?trk=public_profile_<timestamp>
    """
    ts = str(int(time.time() * 1000))
    parsed = up.urlparse(url)
    q = up.parse_qsl(parsed.query, keep_blank_values=True)
    q.append(("trk", f"public_profile_{ts}"))
    new_query = up.urlencode(q)
    new_parsed = parsed._replace(query=new_query)
    return up.urlunparse(new_parsed)


def atomic_write_csv(df: pd.DataFrame, path: Path):
    tmp = path.with_suffix(".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)


def refresh_slack_token() -> str:
    if not SLACK_BOT_TOKEN:
        raise RuntimeError("No hay SLACK_BOT_TOKEN ni SLACK_TOKEN en el .env")
    return SLACK_BOT_TOKEN


def get_replies_with_retry(client: WebClient, channel: str, ts: str, max_retries: int = 3) -> Dict[str, Any]:
    last_exc = None
    for _ in range(max_retries):
        try:
            return client.conversations_replies(channel=channel, ts=ts)
        except SlackApiError as e:
            last_exc = e
            print(f"⚠️ Slack se quejó leyendo replies, reintento... {e.response.get('error')}")
            time.sleep(1.5)
        except Exception as e:
            last_exc = e
            print(f"⚠️ Error de red leyendo replies, reintento... {e}")
            time.sleep(1.5)
    print("❌ No se pudo leer el reply después de varios intentos")
    if last_exc:
        raise last_exc
    return {}


# ─────────────────────────────────────────────────────────────
# CORE: enviar lote → leer unfurls → borrar mensaje
# ─────────────────────────────────────────────────────────────
def post_batch_and_get_unfurls(client: WebClient, urls: List[str]) -> Dict[str, Optional[str]]:
    # aplicamos el patrón bueno a cada url
    posted = [add_probe_param(u) for u in urls]
    text = "\n".join(posted)

    # 1) mandamos el mensaje
    try:
        resp = client.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=text,
            unfurl_links=True,
        )
    except SlackApiError as e:
        print(f"⚠️ Error al enviar lote: {e.response.get('error')}")
        return {u: None for u in urls}
    except Exception as e:
        print(f"⚠️ Error inesperado al enviar lote: {e}")
        return {u: None for u in urls}

    ts = resp["ts"]

    # 2) esperamos a que slack haga el unfurl
    time.sleep(UNFURL_WAIT_SECONDS)

    # 3) leemos las respuestas
    try:
        reply = get_replies_with_retry(client, SLACK_CHANNEL_ID, ts)
    except Exception as e:
        print(f"⚠️ Error al leer unfurls: {e}")
        reply = {}

    msg = (reply.get("messages") or [{}])[0]
    atts = msg.get("attachments", []) if msg else []

    results: Dict[str, Optional[str]] = {u: None for u in urls}

    # 4) mapeo por url normalizada
    for att in atts:
        original_url = (
            att.get("original_url")
            or att.get("title_link")
            or att.get("from_url")
            or ""
        )
        text_fields = " \n ".join(
            str(att.get(k, "")) for k in ("title", "text", "fallback", "pretext")
        ).strip()

        key_norm = normalize_url(original_url)
        if key_norm:
            for u in urls:
                if normalize_url(u) == key_norm:
                    results[u] = text_fields

    # 5) fallback por posición
    if any(v is None for v in results.values()) and atts:
        for idx, att in enumerate(atts):
            if idx >= len(urls):
                break
            if results[urls[idx]] is None:
                text_fields = " \n ".join(
                    str(att.get(k, "")) for k in ("title", "text", "fallback", "pretext")
                ).strip()
                results[urls[idx]] = text_fields

    # 6) borramos el mensaje, pero blindado
    if DELETE_MESSAGES:
        try:
            client.chat_delete(channel=SLACK_CHANNEL_ID, ts=ts)
        except SlackApiError as e:
            print(f"⚠️ No se pudo borrar el mensaje (SlackApiError): {e.response.get('error')}")
        except Exception as e:
            print(f"⚠️ No se pudo borrar el mensaje (error de red/time out): {e}")

    filled = sum(1 for v in results.values() if v)
    print(f"📦 Unfurls: {filled}/{len(urls)}")
    return results


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("🚀 Slack unfurl → columna raw_headline")

    if not SLACK_CHANNEL_ID:
        print("❌ Falta SLACK_CHANNEL_ID en .env")
        return

    access_token = refresh_slack_token()
    client = WebClient(token=access_token)

    # 1) cargar df (reanudar si ya existe)
    if OUT_PATH.exists():
        print(f"📂 Reanudando desde {OUT_PATH.name}")
        df = pd.read_csv(OUT_PATH)
    else:
        print(f"📄 Cargando base desde {CSV_PATH.name}")
        df = pd.read_csv(CSV_PATH)
        if "raw_headline" not in df.columns:
            df["raw_headline"] = pd.Series(dtype="object")
        atomic_write_csv(df, OUT_PATH)

    # 2) construir lista de urls pendientes
    urls_to_do: List[str] = []
    seen = set()
    for _, row in df.iterrows():
        url = row.get("linkedinUrl") or row.get("url") or row.get("profile_url")
        url_norm = normalize_url(url)
        if not url_norm:
            continue
        if url_norm in seen:
            continue
        seen.add(url_norm)

        raw_val = row.get("raw_headline")
        if pd.isna(raw_val) or raw_val == "":
            urls_to_do.append(url_norm)

    if LIMIT_URLS and LIMIT_URLS > 0:
        urls_to_do = urls_to_do[:LIMIT_URLS]

    print(f"📝 URLs pendientes: {len(urls_to_do)}")

    # 3) procesar en lotes, blindado
    batch_idx = 0
    for batch in chunked(urls_to_do, BATCH_SIZE):
        batch_idx += 1
        print(f"\n▶ Lote {batch_idx} — {len(batch)} enlaces")

        try:
            res = post_batch_and_get_unfurls(client, batch)
        except Exception as e:
            print(f"❌ Error en lote {batch_idx}: {e}")
            atomic_write_csv(df, OUT_PATH)
            continue

        # 4) actualizar df con lo que sí llegó
        for i, row in df.iterrows():
            url = row.get("linkedinUrl") or row.get("url") or row.get("profile_url")
            url_norm = normalize_url(url)
            if not url_norm:
                continue
            if url_norm in res and res[url_norm]:
                df.at[i, "raw_headline"] = res[url_norm]

        atomic_write_csv(df, OUT_PATH)
        print(f"💾 Guardado → {OUT_PATH.name}")

        time.sleep(SLEEP_BETWEEN_BATCHES)

    print("✅ Terminado.")


if __name__ == "__main__":
    main()
