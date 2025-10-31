# -*- coding: utf-8 -*-
import psycopg2
from typing import List, Tuple
from dotenv import load_dotenv
import os

load_dotenv()

from harvestapi_dispatch_standalone import harvest_for_urls, normalize_linkedin_url
from json_2_sql import update_items_in_db, DB, SCHEMA

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5"))
MAX_URLS_PER_RUN = int(os.getenv("MAX_URLS_PER_RUN", "5"))  # 0 = sin límite
MIN_CONNECTIONS = int(os.getenv("MIN_CONNECTIONS", "0"))
REFRESH_CHILDREN = os.getenv("REFRESH_CHILDREN", "true").lower() == "true"

def get_pending_urls(limit: int) -> List[Tuple[int, str]]:
    q = f"""
    SELECT profile_id, rtrim(lower(linkedin_url), '/') AS url_norm
    FROM {SCHEMA}.profiles
    WHERE public_identifier IS NULL
      AND linkedin_url IS NOT NULL
      AND connections >= {MIN_CONNECTIONS}
    ORDER BY profile_id
    LIMIT %s;
    """
    conn = psycopg2.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute(q, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    total_limit = MAX_URLS_PER_RUN if MAX_URLS_PER_RUN > 0 else 10**9
    pending = get_pending_urls(total_limit)
    if not pending:
        print("✅ No hay perfiles pendientes (public_identifier IS NULL).")
        return
    print(f"Encontrados {len(pending)} pendientes. CHUNK_SIZE={CHUNK_SIZE}")

    processed = 0
    for batch in chunked(pending, CHUNK_SIZE):
        urls = [normalize_linkedin_url(u) for _, u in batch if u]
        if not urls:
            continue
        items = harvest_for_urls(urls)  # 1) actor

        # --- Marcar como INACCESSIBLE los que devolvieron error ---
        failed_urls = []
        for r in items:
            # Algunos actores devuelven estructura con 'status' o 'error'
            status = r.get("status")
            if status == 403 or r.get("error"):
                query = r.get("query") or {}
                failed_urls.append(query.get("url"))

        if failed_urls:
            print(f"⚠️ {len(failed_urls)} perfiles marcados como INACCESIBLE.")
            try:
                conn = psycopg2.connect(**DB)
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {SCHEMA}")
                    for u in failed_urls:
                        if u:
                            cur.execute("""
                                UPDATE profiles
                                SET public_identifier = 'INACCESIBLE'
                                WHERE LOWER(rtrim(linkedin_url,'/')) = LOWER(rtrim(%s,'/'));
                            """, (u,))
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"Error al marcar INACCESIBLE: {e}")
        # ------------------------------------------------------------

        n = update_items_in_db(items, REFRESH_CHILDREN)  # 2) update por linkedin_url (+ refresh hijos)
        processed += n
        print(f"🧾 Lote listo: {n} perfiles. Acumulado: {processed}")
        if processed >= total_limit:
            print(f"⏹️ Alcanzado MAX_URLS_PER_RUN={MAX_URLS_PER_RUN}.")
            break
    print(f"🎉 Terminado. Perfiles actualizados: {processed}")

if __name__ == "__main__":
    main()
