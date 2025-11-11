import pandas as pd
import re
import time
import os
import subprocess

# ========== CONFIG ==========
INPUT_FILE = r"data\data_for_test_llm.csv"
OUTPUT_FILE = r"data\data_for_test_llm.csv"   # sobre el mismo
MODEL = "phi3"
BATCH_SIZE = 10
SAVE_EVERY = 5
SLEEP_BETWEEN = 1
# ============================

EXCLUDE_TERMS = [
    "camarero", "camarera", "mesero", "ayudante de cocina", "cocinero", "cocinera",
    "chef", "barista", "barman", "bartender", "maître", "hostelería", "restauración",
    "albañil", "peón", "yesero", "pintor", "pintora", "soldador", "operario", "cerrajero",
    "fisioterapeuta", "fisio", "masajista", "osteópata", "acupuntor", "instructor",
    "trainer", "coach", "yoga", "pilates", "fitness", "zumba", "esteticista",
    "manicurista", "peluquero", "peluquera", "barbero", "dependiente", "cajero",
    "cajera", "reponedor", "mozo de almacén", "empaquetador", "teleoperador",
    "call center", "cuidadora", "empleada de hogar", "limpieza", "niñera"
]
EXCLUDE_REGEX = re.compile("|".join(EXCLUDE_TERMS), re.IGNORECASE)


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "descartado_regex" not in df.columns:
        df["descartado_regex"] = pd.NA
    if "llm_relevante" not in df.columns:
        df["llm_relevante"] = pd.NA
    if "relevante_final" not in df.columns:
        df["relevante_final"] = pd.NA
    return df


def is_excluded(headline: str) -> bool:
    return bool(EXCLUDE_REGEX.search(str(headline).lower()))


def query_ollama_cli(headline: str) -> bool:
    """
    Llama a ollama por CLI. En Windows forzamos encoding utf-8 y errors='ignore'
    para evitar UnicodeDecodeError.
    """
    prompt = f"""
Analiza este titular de LinkedIn y responde solo con una palabra: "mantener" o "descartar".
DESCARTAR: hostelería, restauración, cocina, camareros, construcción básica, estética y belleza, fitness/yoga, cuidados y limpieza.
MANTENER: perfiles corporativos, técnicos, ingeniería, data, IT, management, marketing, ventas B2B.
Titular: "{headline}"
Responde solo la palabra.
""".strip()

    try:
        result = subprocess.run(
            ["ollama", "run", MODEL, prompt],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="ignore",   # <- clave
            timeout=90
        )
        text = (result.stdout or "").strip().lower()
        if "mantener" in text:
            return True
        return False
    except Exception as e:
        print("⚠️ Error llamando a ollama por CLI:", e)
        return False


def main():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"No encontré el archivo {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)
    if "raw_headline" not in df.columns:
        raise ValueError("El CSV no tiene la columna 'raw_headline'")

    df = ensure_columns(df)

    # aplicar regex donde falte
    mask_no_regex = df["descartado_regex"].isna()
    df.loc[mask_no_regex, "descartado_regex"] = df.loc[mask_no_regex, "raw_headline"].apply(is_excluded)

    # pendientes de LLM
    to_process_mask = (~df["descartado_regex"].astype(bool)) & (df["llm_relevante"].isna())
    to_process_idx = df[to_process_mask].index.tolist()

    print(f"Total filas: {len(df)}")
    print(f"Ya descartadas por regex: {df['descartado_regex'].astype(bool).sum()}")
    print(f"Pendientes de LLM: {len(to_process_idx)}")

    lotes_procesados = 0

    try:
        for start in range(0, len(to_process_idx), BATCH_SIZE):
            batch_idx = to_process_idx[start:start+BATCH_SIZE]
            print(f"\n➡️ Lote {start//BATCH_SIZE + 1} ({len(batch_idx)} filas)")

            for idx in batch_idx:
                headline = df.at[idx, "raw_headline"]
                llm_ok = query_ollama_cli(headline)
                df.at[idx, "llm_relevante"] = llm_ok

            lotes_procesados += 1
            if lotes_procesados % SAVE_EVERY == 0:
                df["relevante_final"] = (~df["descartado_regex"].astype(bool)) & (df["llm_relevante"] == True)
                df.to_csv(OUTPUT_FILE, index=False)
                print(f"💾 Guardado parcial en {OUTPUT_FILE}")

            time.sleep(SLEEP_BETWEEN)

    except KeyboardInterrupt:
        # si lo paras con Ctrl+C, guardamos lo que haya
        print("\n⛔ Interrumpido por el usuario. Guardando progreso...")
        df["relevante_final"] = (~df["descartado_regex"].astype(bool)) & (df["llm_relevante"] == True)
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"💾 Progreso guardado en {OUTPUT_FILE}")
        return

    # guardado final
    df["relevante_final"] = (~df["descartado_regex"].astype(bool)) & (df["llm_relevante"] == True)
    df.to_csv(OUTPUT_FILE, index=False)

    print("\n✅ Proceso terminado.")
    print(f"Relevantes finales: {df['relevante_final'].sum()} / {len(df)}")


if __name__ == "__main__":
    main()
