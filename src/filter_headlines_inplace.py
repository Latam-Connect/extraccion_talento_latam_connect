# filter_headlines_inplace_v22.py
# Filtro léxico multilingüe con co-ocurrencia (v2.2)

import pandas as pd
import unicodedata, regex as re
from pathlib import Path

# =========================
# CONFIG
# =========================
CONFIG = {
    "input_csv": r"data\data_for_test_only_filter.csv",
    "output_csv": r"data\data_for_test_only_filter_result.csv",
    "headline_col": "raw_headline",
    "umbral": 2.5,         # exigencia para pasar
    "include_bias": 0.0,   # empujón positivo para tech
    "save_rejects_csv": True,
    "rejects_csv": r"data\data_for_test_only_filter_reject.csv",
}

# =========================
# Diccionarios v2.2
# =========================
INCLUDE_PHRASES = [
    # CORE DATA / ML / AI
    ("data scientist", 7), ("cientifico de datos", 7),
    ("data analyst", 6), ("analista de datos", 6),
    ("data engineer", 7), ("ingeniero de datos", 7),
    ("machine learning", 7), ("aprendizaje automatico", 7),
    ("ml engineer", 7), ("deep learning", 6), ("nlp", 5),
    ("business intelligence", 5), ("bi developer", 5), ("power bi", 5), ("tableau", 4), ("qlik", 4),
    ("analytics engineer", 6), ("estadistico", 5), ("statistician", 5),
    ("azure data", 5), ("aws data", 5), ("gcp data", 5), ("big data", 5),
    ("looker", 3), ("dbt", 4), ("airflow", 4), ("snowflake", 4), ("dax", 3), ("power query", 3),
    # SOFTWARE / DEV / OPS
    ("software engineer", 7), ("ingeniero de software", 7), ("software developer", 6),
    ("desarrollador", 6), ("programador", 6),
    ("backend", 4), ("frontend", 4), ("full stack", 6), ("fullstack", 6),
    ("devops", 6), ("site reliability", 6), ("sre", 5),
    ("qa engineer", 5), ("quality assurance", 4), ("test automation", 5),
    ("kubernetes", 5), ("docker", 4), ("microservices", 4),
    # TECNOLOGÍAS extra (para atrapar más tech genuino)
    (".net", 4), ("dotnet", 4), ("asp.net", 4), ("mvc", 2),
    ("sql server", 4), ("mysql", 3), ("postgres", 3), ("oracle", 3), ("pl/sql", 3),
    ("ssis", 3), ("ssrs", 3),
    ("django", 3), ("flask", 3), ("spring", 3), ("spring boot", 4),
    ("angular", 3), ("vue", 3),
]

# Lenguajes: peso bajo + co-ocurrencia con rol
LANG_TOKENS = [
    ("python", 2), ("c#", 1.5), ("java", 2), ("javascript", 2),
    ("typescript", 2), ("go", 2), ("php", 2), ("c++", 2), ("c", 1.5), ("scala", 2), ("r", 1.5)
]

# Palabras de ROL (para co-ocurrencia)
ROLE_TOKENS = [
    "developer","engineer","software","desarrollador","programador","ingeniero",
    "devops","sre","qa","dev","desarrollo","programacion","informatico","sistemas","it","tecnologia"
]

EXCLUDE_PHRASES = [
    # HOSTELERÍA / RETAIL / SERVICIOS
    ("camarero", 8), ("mesero", 8), ("waiter", 8), ("bartender", 8), ("barista", 7),
    ("hosteleria", 6), ("mozo de almacen", 8), ("almacenista", 7), ("warehouse", 7),
    ("reponedor", 8), ("dependiente", 8), ("retail", 7), ("cajero", 8), ("cashier", 8),
    ("limpieza", 8), ("cleaner", 8), ("housekeeping", 8), ("cuidador", 7), ("caregiver", 7),
    # CONSTRUCCIÓN / OFICIOS
    ("albanil", 9), ("bricklayer", 9), ("fontanero", 8), ("plumber", 8),
    ("pintor", 7), ("carpintero", 8), ("obrero", 8), ("construction worker", 9),
    # LOGÍSTICA / REPARTO / TRANSPORTE
    ("repartidor", 8), ("delivery", 7), ("rider", 7), ("uber", 6), ("cabify", 6),
    ("conductor", 8), ("chofer", 8), ("truck driver", 8), ("transportista", 8),
    # CALL CENTER / COMERCIAL / BACKOFFICE
    ("teleoperador", 8), ("call center", 8), ("atencion al cliente", 7),
    ("comercial", 7), ("sales associate", 7), ("promotor", 7), ("ventas", 6),
    ("administrativo", 7), ("auxiliar administrativo", 8), ("asistente", 6), ("assistant", 6),
    ("secretaria", 7), ("recepcionista", 7),
    ("marketing", 5), ("community manager", 7), ("social media", 6),
    ("business development", 6), ("representante", 7),
    # COCINA
    ("cocinero", 8), ("cook", 8), ("chef", 8), ("ayudante de cocina", 9),
    # EDUCACIÓN NO-TECH
    ("profesor de primaria", 7), ("teacher assistant", 7),
    # ELECTRICIDAD (tu caso)
    ("electricista", 8), ("ingeniero electricista", 9),
    # SALUD NO-TECH reforzado
    ("enfermera", 9), ("fisioterapeuta", 9), ("odontologo", 9), ("medico", 9), ("doctor", 9),
]

NEUTRAL_UP = [("consultor", 1), ("consultant", 1), ("analyst", 1), ("manager", 1), ("product", 1), ("project", 1)]
NEUTRAL_DOWN = [("generalista", -1), ("multitarea", -1), ("aprendiz", -0.5), ("trainee", -0.5)]

HARD_EXCLUDES = [("porn",), ("onlyfans",), ("escort",), ("trader de forex",), ("cripto estafa",)]

# =========================
# Utils
# =========================
def strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = strip_accents(s)
    s = re.sub(r"[^\p{L}\p{N}\s\+\#\.\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def compile_phrases(phrases):
    compiled = []
    for p, w in phrases:
        p_norm = normalize_text(p)
        boundary = r"(?<!\w)|(?=\W)" if re.search(r"[#+.\-]", p_norm) else r"\b"
        pattern = re.compile(boundary + re.escape(p_norm) + boundary)
        compiled.append((pattern, w, p))
    return compiled

INCLUDE_RX = compile_phrases(INCLUDE_PHRASES)
EXCLUDE_RX = compile_phrases(EXCLUDE_PHRASES)
UP_RX      = compile_phrases(NEUTRAL_UP)
DOWN_RX    = compile_phrases(NEUTRAL_DOWN)
LANG_RX    = compile_phrases(LANG_TOKENS)
HARD_RX    = [re.compile(re.escape(normalize_text(term))) for (term,) in HARD_EXCLUDES]

# =========================
# Scoring con co-ocurrencia
# =========================
def score_headline(h: str, include_bias=0.0):
    text = normalize_text(h)
    if not text:
        return 0.0, [], [], [], [], False

    for hrx in HARD_RX:
        if hrx.search(text):
            return -999.0, [], [], [], [], False

    incl, excl, up, down, lang_hits = [], [], [], [], []

    def apply(rx_list, bucket):
        for rx, w, original in rx_list:
            if rx.search(text):
                bucket.append((original, w))

    apply(INCLUDE_RX, incl)
    apply(EXCLUDE_RX, excl)
    apply(UP_RX, up)
    apply(DOWN_RX, down)
    apply(LANG_RX, lang_hits)

    has_role = any(re.search(rf"\b{r}\b", text) for r in ROLE_TOKENS)
    lang_score = sum(w for _, w in lang_hits)

    # Penaliza lenguaje sin rol
    if lang_hits and not has_role:
        lang_score *= 0.6   # suavizado
        down.append(("no_role_with_language", -1.0))
    # Bonus si hay lenguaje + rol
    if lang_hits and has_role:
        up.append(("language_with_role_bonus", 1.5))

    score = sum(w for _, w in incl) - sum(w for _, w in excl) + sum(w for _, w in up) + sum(w for _, w in down) + lang_score
    score += include_bias
    return score, incl, excl, up, down, has_role

def decide_keep(score, incl, excl, umbral=2.5):
    hard_excl = any(w >= 8 for _, w in excl)
    if hard_excl and sum(w for _, w in incl) < 7:
        return False
    return score >= umbral

# =========================
# Run
# =========================
def run():
    in_csv = CONFIG["input_csv"]
    out_csv = CONFIG["output_csv"]
    col = CONFIG["headline_col"]
    umbral = CONFIG["umbral"]
    include_bias = CONFIG["include_bias"]

    df = pd.read_csv(in_csv)
    if col not in df.columns:
        raise SystemExit(f"No encuentro la columna '{col}' en {in_csv}")

    keep_list, scores, incl_hits, excl_hits, notes = [], [], [], [], []

    for h in df[col].astype(str).fillna(""):
        score, incl, excl, up, down, has_role = score_headline(h, include_bias=include_bias)
        keep = decide_keep(score, incl, excl, umbral=umbral)
        keep_list.append(keep)
        scores.append(score)
        incl_hits.append("; ".join([f"{t}({w})" for t, w in incl] + [f"{t}({w})" for t, w in up]))
        excl_hits.append("; ".join([f"{t}({w})" for t, w in excl] + [f"{t}({w})" for t, w in down]))
        notes.append("no_role" if (not has_role) else "")

    out = df.copy()
    out["keep"] = keep_list
    out["score"] = scores
    out["incl_hits"] = incl_hits
    out["excl_hits"] = excl_hits
    out["notes"] = notes

    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)

    kept = sum(keep_list)
    total = len(keep_list)
    ratio = kept/total if total else 0.0
    print(f"✅ Guardado → {out_csv} | Mantenidos: {kept}/{total} ({ratio:.1%})  | Umbral={umbral} Bias={include_bias}")

    if CONFIG.get("save_rejects_csv", False):
        rejects = out[~out["keep"]].copy()
        rpath = CONFIG.get("rejects_csv", "filtrado_rechazados.csv")
        rejects.to_csv(rpath, index=False)
        print(f"🗂️  Rechazados guardados en → {rpath}")

if __name__ == "__main__":
    run()
