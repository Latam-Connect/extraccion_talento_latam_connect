# -*- coding: utf-8 -*-
import json, psycopg2, re, unicodedata, traceback
from datetime import date
from typing import Iterable, Dict, Any, Optional, Tuple
from dotenv import load_dotenv
import os

load_dotenv()

DB = dict(
    host=os.getenv("PG_HOST"),
    port=int(os.getenv("PG_PORT")),
    dbname=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    sslmode=os.getenv("PG_SSLMODE"),
)
SCHEMA = os.getenv("PG_SCHEMA")
COMMIT_EVERY = 50

# -------- Normalizaciones --------
MONTHS = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
    "ene":1,"enero":1,"feb":2,"febrero":2,"mar":3,"marzo":3,"abr":4,"abril":4,
    "may":5,"jun":6,"junio":6,"jul":7,"julio":7,"ago":8,"agosto":8,"sep":9,"sept":9,
    "septiembre":9,"oct":10,"octubre":10,"nov":11,"noviembre":11,"dic":12,"diciembre":12
}

def _strip(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    s = s.strip()
    return s if s else None

def clean_text(s: Optional[str]) -> Optional[str]:
    s = _strip(s)
    if s is None: return None
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s)
    return s

norm_txt = clean_text

def first_non_empty(*args):
    for a in args:
        a = clean_text(a)
        if a:
            return a
    return None

def strip_accents(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def normalize_language_name(raw: Optional[str]) -> Optional[str]:
    s = clean_text(raw)
    if not s: return None
    s_low = strip_accents(s.lower())
    mapping = {
        "es": "spanish", "esp": "spanish", "espanol": "spanish", "spanish": "spanish",
        "en": "english", "ing": "english", "ingles": "english", "english": "english",
        "pt": "portuguese", "portugues": "portuguese", "portuguese": "portuguese",
        "fr": "french", "frances": "french", "french": "french",
        "de": "german", "aleman": "german", "german": "german",
        "it": "italian", "italiano": "italian", "italian": "italian"
    }
    return mapping.get(s_low, s_low)

def normalize_linkedin_url(u: Optional[str]) -> Optional[str]:
    if not u: return None
    u = u.strip().split("#", 1)[0]
    u = u.rstrip("/")
    u = u.lower()
    u = u.replace("www.public.com", "www.linkedin.com")
    return u

def parse_date(obj: Any) -> Optional[date]:
    """
    Acepta:
      - {"year": 2021, "month": 5}
      - {"monthName":"Enero","year":2020}
      - "2020-05", "2020"
    """
    if obj is None:
        return None
    if isinstance(obj, dict):
        y = obj.get("year")
        m = obj.get("month")
        mn = obj.get("monthName")
        if m is None and mn:
            m = MONTHS.get(strip_accents(str(mn).lower()), None)
        try:
            y = int(y) if y is not None else None
            m = int(m) if m is not None else 1
            if y:
                return date(y, max(1, min(12, m)), 1)
        except Exception:
            return None
    if isinstance(obj, str):
        s = obj.strip()
        m = re.match(r"^(\d{4})-(\d{1,2})", s)
        if m:
            return date(int(m.group(1)), max(1, min(12, int(m.group(2)))), 1)
        if re.match(r"^\d{4}$", s):
            return date(int(s), 1, 1)
    return None

# -------- Helpers de catálogo con caches (evitan duplicados) --------
def ensure_location(cur, cache, name):
    name = clean_text(name)
    if not name:
        return None
    key = name.lower()
    if key in cache:
        return cache[key]

    cur.execute(f"""
        SELECT location_id FROM {SCHEMA}.locations
        WHERE LOWER(location_name)=LOWER(%s) LIMIT 1
    """, (name,))
    row = cur.fetchone()
    if row:
        cache[key] = row[0]; return row[0]

    cur.execute(f"""
        INSERT INTO {SCHEMA}.locations(location_name)
        VALUES (%s) RETURNING location_id
    """, (name,))
    lid = cur.fetchone()[0]
    cache[key] = lid
    return lid

def ensure_company(cur, cache, name, link, location_id=None):
    name = clean_text(name) or "(sin nombre)"
    link = clean_text(link)
    key = (name.lower(), (link or "").lower() if link else "")
    if key in cache:
        return cache[key]

    # 1) Si hay link, busca por link (case-insensitive)
    if link:
        cur.execute(f"""
            SELECT company_id
            FROM {SCHEMA}.companies
            WHERE LOWER(company_link)=LOWER(%s)
            LIMIT 1
        """, (link,))
        row = cur.fetchone()
        if row:
            cid = row[0]
            # opcional: rellena campos vacíos si ahora tenemos más info
            cur.execute(f"""
                UPDATE {SCHEMA}.companies
                SET company_name = COALESCE(NULLIF(%s,''), company_name),
                    location_id  = COALESCE(%s, location_id)
                WHERE company_id = %s
            """, (name, location_id, cid))
            cache[key] = cid
            return cid

    # 2) Sin link o no encontrado → intenta por nombre
    cur.execute(f"""
        SELECT company_id
        FROM {SCHEMA}.companies
        WHERE LOWER(company_name)=LOWER(%s)
        LIMIT 1
    """, (name,))
    row = cur.fetchone()
    if row:
        cid = row[0]
        # si ahora tenemos link y la fila no lo tenía, complétalo
        if link:
            cur.execute(f"""
                UPDATE {SCHEMA}.companies
                SET company_link = COALESCE(company_link, %s),
                    location_id  = COALESCE(%s, location_id)
                WHERE company_id = %s
            """, (link, location_id, cid))
        cache[key] = cid
        return cid

    # 3) No existe → inserta
    cur.execute(f"""
        INSERT INTO {SCHEMA}.companies (company_name, company_link, location_id)
        VALUES (%s,%s,%s)
        RETURNING company_id
    """, (name, link, location_id))
    cid = cur.fetchone()[0]
    cache[key] = cid
    return cid


def ensure_school(cur, cache, name, link, location_id=None):
    name = clean_text(name) or "(sin nombre)"
    link = clean_text(link)
    key = (name.lower(), (link or "").lower() if link else "")
    if key in cache:
        return cache[key]

    # 1) Si hay link, busca por link
    if link:
        cur.execute(f"""
            SELECT school_id
            FROM {SCHEMA}.educational_institutions
            WHERE LOWER(school_link)=LOWER(%s)
            LIMIT 1
        """, (link,))
        row = cur.fetchone()
        if row:
            sid = row[0]
            cur.execute(f"""
                UPDATE {SCHEMA}.educational_institutions
                SET school_name = COALESCE(NULLIF(%s,''), school_name),
                    location_id = COALESCE(%s, location_id)
                WHERE school_id = %s
            """, (name, location_id, sid))
            cache[key] = sid
            return sid

    # 2) Por nombre si no hay link o no se encontró
    cur.execute(f"""
        SELECT school_id
        FROM {SCHEMA}.educational_institutions
        WHERE LOWER(school_name)=LOWER(%s)
        LIMIT 1
    """, (name,))
    row = cur.fetchone()
    if row:
        sid = row[0]
        if link:
            cur.execute(f"""
                UPDATE {SCHEMA}.educational_institutions
                SET school_link = COALESCE(school_link, %s),
                    location_id = COALESCE(%s, location_id)
                WHERE school_id = %s
            """, (link, location_id, sid))
        cache[key] = sid
        return sid

    # 3) Inserta
    cur.execute(f"""
        INSERT INTO {SCHEMA}.educational_institutions (school_name, school_link, location_id)
        VALUES (%s,%s,%s)
        RETURNING school_id
    """, (name, link, location_id))
    sid = cur.fetchone()[0]
    cache[key] = sid
    return sid


def ensure_language(cur, cache, lang):
    lang = normalize_language_name(lang)
    if not lang:
        return None
    key = lang.lower()
    if key in cache:
        return cache[key]

    cur.execute(f"""
        SELECT lang_id FROM {SCHEMA}.languages
        WHERE LOWER(language)=LOWER(%s) LIMIT 1
    """, (lang,))
    row = cur.fetchone()
    if row:
        cache[key] = row[0]; return row[0]

    cur.execute(f"""
        INSERT INTO {SCHEMA}.languages(language)
        VALUES (%s) RETURNING lang_id
    """, (lang,))
    lid = cur.fetchone()[0]
    cache[key] = lid
    return lid

def ensure_skill(cur, cache, skill):
    skill = clean_text(skill)
    if not skill:
        return None
    key = skill.lower()
    if key in cache:
        return cache[key]

    cur.execute(f"""
        SELECT skill_id FROM {SCHEMA}.skills
        WHERE LOWER(skill_name)=LOWER(%s) LIMIT 1
    """, (skill,))
    row = cur.fetchone()
    if row:
        cache[key] = row[0]; return row[0]

    cur.execute(f"""
        INSERT INTO {SCHEMA}.skills(skill_name)
        VALUES (%s) RETURNING skill_id
    """, (skill,))
    sid = cur.fetchone()[0]
    cache[key] = sid
    return sid

# -------- Borrado de hijos (para refresh) --------
def delete_children_for_profile(cur, profile_id: int):
    cur.execute(f'DELETE FROM {SCHEMA}.profile_skills WHERE profile_id=%s', (profile_id,))
    cur.execute(f'DELETE FROM {SCHEMA}.profile_languages WHERE profile_id=%s', (profile_id,))
    cur.execute(f'DELETE FROM {SCHEMA}.educations WHERE profile_id=%s', (profile_id,))
    cur.execute(f'DELETE FROM {SCHEMA}.experiences WHERE profile_id=%s', (profile_id,))

# -------- Upsert principal desde items --------
def update_from_items(cur, items: Iterable[Dict[str, Any]], refresh_children: bool = True) -> int:
    loc_cache, comp_cache, school_cache, lang_cache, skill_cache = {}, {}, {}, {}, {}
    total = 0
    for p in items or []:
        linkedin_url      = normalize_linkedin_url(p.get("linkedinUrl"))
        if not linkedin_url:
            continue
        public_identifier = norm_txt(p.get("publicIdentifier"))
        first_name        = norm_txt(p.get("firstName"))
        last_name         = norm_txt(p.get("lastName"))
        headline          = norm_txt(p.get("headline"))
        about             = norm_txt(p.get("about"))
        connections       = p.get("connectionsCount")
        followers         = p.get("followerCount")

        loc_name = first_non_empty(
            (p.get("location") or {}).get("parsed", {}).get("text"),
            (p.get("location") or {}).get("linkedinText")
        )
        location_id = ensure_location(cur, loc_cache, loc_name) if loc_name else None

        # UPSERT de profile por linkedin_url
        cur.execute(f"""
            INSERT INTO {SCHEMA}.profiles
                (public_identifier, linkedin_url, first_name, last_name, headline, about,
                 connections, followers, location_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (linkedin_url) DO UPDATE
              SET public_identifier = COALESCE(EXCLUDED.public_identifier, {SCHEMA}.profiles.public_identifier),
                  first_name = COALESCE(EXCLUDED.first_name, {SCHEMA}.profiles.first_name),
                  last_name  = COALESCE(EXCLUDED.last_name,  {SCHEMA}.profiles.last_name),
                  headline   = COALESCE(EXCLUDED.headline,   {SCHEMA}.profiles.headline),
                  about      = COALESCE(EXCLUDED.about,      {SCHEMA}.profiles.about),
                  connections= COALESCE(EXCLUDED.connections,{SCHEMA}.profiles.connections),
                  followers  = COALESCE(EXCLUDED.followers,  {SCHEMA}.profiles.followers),
                  location_id= COALESCE(EXCLUDED.location_id, {SCHEMA}.profiles.location_id)
            RETURNING profile_id
        """, (public_identifier, linkedin_url, first_name, last_name, headline, about,
              connections, followers, location_id))
        row = cur.fetchone()
        if not row:
            cur.execute(f"SELECT profile_id FROM {SCHEMA}.profiles WHERE linkedin_url=%s LIMIT 1",(linkedin_url,))
            row = cur.fetchone()
        profile_id = row[0]

        if refresh_children:
            delete_children_for_profile(cur, profile_id)

        # EXPERIENCES
        for e in (p.get("experience") or []):
            company_name = norm_txt(e.get("companyName"))
            company_link = norm_txt(e.get("companyLinkedinUrl"))
            if company_link and "/company/" not in company_link:
                company_link = None
            exp_loc_name = norm_txt(e.get("location"))
            exp_location_id = ensure_location(cur, loc_cache, exp_loc_name) if exp_loc_name else None
            company_id = ensure_company(cur, comp_cache, company_name, company_link, None)

            title = norm_txt(e.get("position"))
            description = norm_txt(e.get("description"))
            start_date = parse_date(e.get("startDate"))
            end_date   = parse_date(e.get("endDate"))
            if start_date and end_date and end_date < start_date:
                end_date = start_date

            cur.execute(f"""
                INSERT INTO {SCHEMA}.experiences
                    (profile_id, company_id, title, description, start_date, end_date, location_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (profile_id, company_id, title, description, start_date, end_date, exp_location_id))

            for sk in (e.get("skills") or []):
                sid = ensure_skill(cur, skill_cache, norm_txt(sk))
                if sid:
                    cur.execute(f"""
                        INSERT INTO {SCHEMA}.profile_skills (profile_id, skill_id)
                        VALUES (%s,%s) ON CONFLICT DO NOTHING
                    """, (profile_id, sid))

        # EDUCATIONS
        for ed in (p.get("education") or []):
            school_name = norm_txt(ed.get("schoolName"))
            school_link = norm_txt(ed.get("schoolLinkedinUrl"))
            school_id = ensure_school(cur, school_cache, school_name, school_link, None)
            title = " ".join([t for t in [norm_txt(ed.get("degree")), norm_txt(ed.get("fieldOfStudy"))] if t])
            start_date = parse_date(ed.get("startDate"))
            end_date   = parse_date(ed.get("endDate"))
            if start_date and end_date and end_date < start_date:
                end_date = start_date
            cur.execute(f"""
                INSERT INTO {SCHEMA}.educations
                    (profile_id, school_id, title, description, start_date, end_date, location_id)
                VALUES (%s,%s,%s,%s,%s,%s,NULL)
            """, (profile_id, school_id, title, None, start_date, end_date))

        # LANGUAGES
        for lg in (p.get("languages") or []):
            lname = normalize_language_name(lg.get("name"))
            level = norm_txt(lg.get("proficiency"))
            lid = ensure_language(cur, lang_cache, lname)
            if lid:
                cur.execute(f"""
                    INSERT INTO {SCHEMA}.profile_languages (profile_id, lang_id, level)
                    VALUES (%s,%s,%s)
                    ON CONFLICT (profile_id, lang_id) DO UPDATE SET level = EXCLUDED.level
                """, (profile_id, lid, level))

        # SKILLS del perfil
        for s in (p.get("skills") or []):
            sname = norm_txt(s.get("name"))
            sid = ensure_skill(cur, skill_cache, sname)
            if sid:
                cur.execute(f"""
                    INSERT INTO {SCHEMA}.profile_skills (profile_id, skill_id)
                    VALUES (%s,%s) ON CONFLICT DO NOTHING
                """, (profile_id, sid))

        total += 1
        if total % COMMIT_EVERY == 0:
            cur.connection.commit()
            print(f"Committed {total} perfiles...")
    return total

def update_items_in_db(items: Iterable[Dict[str, Any]], refresh_children=True) -> int:
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        n = update_from_items(cur, items, refresh_children=refresh_children)
        conn.commit()
        return n
    except Exception:
        conn.rollback()
        print("❌ ERROR en update_items_in_db")
        print(traceback.format_exc())
        raise
    finally:
        cur.close(); conn.close()
