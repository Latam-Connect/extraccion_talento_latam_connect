# -*- coding: utf-8 -*-
"""
inspect_profile_v2.py
---------------------
Versión mejorada:
- Usa SQLAlchemy para evitar warnings de pandas.
- Exporta JSON sin errores (convierte Timestamp/Date/Interval/NaT/NumPy/etc.).
- Permite inspeccionar por --id o --url y exportar con --out.

Uso:
  python inspect_profile_v2.py --id 4067
  python inspect_profile_v2.py --url "https://www.linkedin.com/in/alguien"
  python inspect_profile_v2.py --id 4067 --out dossier_4067.json
"""

import os
import sys
import json
import argparse
from typing import Optional, Any, Dict

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import datetime as _dt
import numpy as _np
from decimal import Decimal

load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_SSLMODE = os.getenv("PG_SSLMODE", "require")

def make_engine() -> Engine:
    # Nota: escapamos '@' en el user si fuese necesario; en tu caso PG_USER es 'sergio'
    uri = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}?sslmode={PG_SSLMODE}"
    return create_engine(uri, pool_pre_ping=True)

def norm_url(u: str) -> str:
    if not u:
        return u
    u = u.strip().split("#", 1)[0]
    if u.endswith("/"):
        u = u[:-1]
    return u.lower()

def get_profile_id_by_url(engine: Engine, url: str) -> Optional[int]:
    q = text("""
        SELECT profile_id
        FROM public.profiles
        WHERE LOWER(rtrim(linkedin_url,'/')) = LOWER(rtrim(:u,'/'))
        LIMIT 1;
    """)
    with engine.connect() as conn:
        row = conn.execute(q, {"u": url}).fetchone()
        return int(row[0]) if row else None

def fetch_df(engine: Engine, sql: str, params: Dict[str, Any]) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)

def fetch_profile(engine: Engine, pid: int) -> pd.DataFrame:
    return fetch_df(engine, "SELECT * FROM public.profiles WHERE profile_id = :pid;", {"pid": pid})

def fetch_experiences(engine: Engine, pid: int) -> pd.DataFrame:
    sql = """
    SELECT
      e.experience_id,
      e.title,
      e.description,
      e.start_date,
      e.end_date,
      e.period,
      e.company_id,
      c.company_name,
      c.company_link,
      e.location_id
    FROM public.experiences e
    LEFT JOIN public.companies c ON c.company_id = e.company_id
    WHERE e.profile_id = :pid
    ORDER BY e.start_date NULLS LAST;
    """
    try:
        return fetch_df(engine, sql, {"pid": pid})
    except Exception:
        sql2 = """
        SELECT
          e.experience_id,
          e.title,
          e.description,
          e.start_date,
          e.end_date,
          e.period,
          e.company_id,
          e.location_id
        FROM public.experiences e
        WHERE e.profile_id = :pid
        ORDER BY e.start_date NULLS LAST;
        """
        return fetch_df(engine, sql2, {"pid": pid})

def fetch_educations(engine: Engine, pid: int) -> pd.DataFrame:
    sql = """
    SELECT
      ed.education_id,
      ed.title,
      ed.description,
      ed.start_date,
      ed.end_date,
      ed.period,
      ed.school_id,
      ei.school_name,
      ei.school_link,
      ed.location_id
    FROM public.educations ed
    LEFT JOIN public.educational_institutions ei ON ei.school_id = ed.school_id
    WHERE ed.profile_id = :pid
    ORDER BY ed.start_date NULLS LAST;
    """
    try:
        return fetch_df(engine, sql, {"pid": pid})
    except Exception:
        sql2 = """
        SELECT
          ed.education_id,
          ed.title,
          ed.description,
          ed.start_date,
          ed.end_date,
          ed.period,
          ed.school_id,
          ed.location_id
        FROM public.educations ed
        WHERE ed.profile_id = :pid
        ORDER BY ed.start_date NULLS LAST;
        """
        return fetch_df(engine, sql2, {"pid": pid})

def fetch_languages(engine: Engine, pid: int) -> pd.DataFrame:
    sql = """
    SELECT
      l.language,
      pl.level
    FROM public.profile_languages pl
    JOIN public.languages l ON l.lang_id = pl.lang_id
    WHERE pl.profile_id = :pid
    ORDER BY l.language;
    """
    return fetch_df(engine, sql, {"pid": pid})

def fetch_skills(engine: Engine, pid: int) -> pd.DataFrame:
    sql = """
    SELECT
      s.skill_name
    FROM public.profile_skills ps
    JOIN public.skills s ON s.skill_id = ps.skill_id
    WHERE ps.profile_id = :pid
    ORDER BY s.skill_name;
    """
    return fetch_df(engine, sql, {"pid": pid})

def fetch_coverage(engine: Engine, pid: int) -> pd.DataFrame:
    sql = """
    SELECT
      (SELECT COUNT(*) FROM public.experiences       e  WHERE e.profile_id = :pid) AS n_experiences,
      (SELECT COUNT(*) FROM public.educations        ed WHERE ed.profile_id = :pid) AS n_educations,
      (SELECT COUNT(*) FROM public.profile_languages pl WHERE pl.profile_id = :pid) AS n_languages,
      (SELECT COUNT(*) FROM public.profile_skills    ps WHERE ps.profile_id = :pid) AS n_skills;
    """
    return fetch_df(engine, sql, {"pid": pid})

# ---------- JSON helpers ----------
def _to_builtin(v: Any):
    # Manejo de tipos molestos para JSON
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (Decimal,)):
        return float(v)
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.isoformat()
    if isinstance(v, pd.Timestamp):
        # Incluye tz si la hay
        return v.isoformat()
    if isinstance(v, pd.Timedelta):
        return str(v)
    if isinstance(v, pd.Interval):
        return str(v)
    # numpy
    if isinstance(v, _np.integer):
        return int(v)
    if isinstance(v, _np.floating):
        return float(v)
    if isinstance(v, _np.bool_):
        return bool(v)
    # Rango/otros de psycopg: str() suele bastar
    try:
        return str(v)
    except Exception:
        return None

def df_to_records(df: pd.DataFrame):
    if df is None or df.empty:
        return []
    # Convertimos NaT a None
    df = df.where(pd.notnull(df), None)
    recs = df.to_dict(orient="records")
    # Normalizamos valores
    for r in recs:
        for k, v in list(r.items()):
            r[k] = _to_builtin(v)
    return recs

def to_dossier(profile_df, exp_df, edu_df, lang_df, skill_df):
    dossier = {
        "profile": df_to_records(profile_df)[0] if not profile_df.empty else None,
        "experiences": df_to_records(exp_df),
        "educations": df_to_records(edu_df),
        "languages": df_to_records(lang_df),
        "skills": df_to_records(skill_df),
    }
    return dossier

def main():
    ap = argparse.ArgumentParser(description="Inspecciona un perfil por profile_id o linkedin_url.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--id", type=int, help="profile_id")
    g.add_argument("--url", type=str, help="linkedin_url")
    ap.add_argument("--out", type=str, help="Ruta de salida para JSON (opcional)")
    args = ap.parse_args()

    engine = make_engine()
    pid = args.id
    if args.url:
        url = norm_url(args.url)
        pid = get_profile_id_by_url(engine, url)
        if pid is None:
            print(f"❌ No se encontró profile_id para la URL: {args.url}")
            sys.exit(2)

    # Fetch
    df_profile = fetch_profile(engine, pid)
    df_exp = fetch_experiences(engine, pid)
    df_edu = fetch_educations(engine, pid)
    df_lang = fetch_languages(engine, pid)
    df_skill = fetch_skills(engine, pid)
    df_cov = fetch_coverage(engine, pid)

    # Pretty print
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 140)

    print("\n📌 PROFILE")
    print(df_profile if not df_profile.empty else "(sin datos)")

    print("\n🧰 EXPERIENCES")
    print(df_exp if not df_exp.empty else "(sin datos)")

    print("\n🎓 EDUCATIONS")
    print(df_edu if not df_edu.empty else "(sin datos)")

    print("\n🗣️ LANGUAGES")
    print(df_lang if not df_lang.empty else "(sin datos)")

    print("\n🧠 SKILLS")
    print(df_skill if not df_skill.empty else "(sin datos)")

    print("\n✅ COVERAGE")
    print(df_cov)

    dossier = to_dossier(df_profile, df_exp, df_edu, df_lang, df_skill)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(dossier, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Dossier exportado a: {args.out}")

if __name__ == "__main__":
    main()
