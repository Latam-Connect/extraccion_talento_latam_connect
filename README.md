# ⚙️ Orquestador de Extracción — Proyecto LATAM Connect

Este módulo controla el flujo de actualización de perfiles de **LinkedIn** dentro de la base de datos **PostgreSQL**.  
Selecciona los perfiles pendientes (sin `public_identifier`), los procesa en lotes y actualiza la información usando el actor de **Apify/HarvestAPI**.

Además, marca automáticamente con `INACCESIBLE` aquellos perfiles que devuelven errores (por ejemplo, 403 — perfil no accesible sin login).

---

## 🚀 Instrucciones de uso

### 📂 Ruta del archivo principal

El archivo principal se encuentra en:

```
src/orchestrate_from_db.py
```

---

## 🧩 Funcionalidad principal

1. Selecciona los perfiles pendientes de la tabla `profiles`:
   ```sql
   SELECT profile_id, linkedin_url
   FROM linkedin.profiles
   WHERE public_identifier IS NULL
     AND linkedin_url IS NOT NULL
     AND connections >= MIN_CONNECTIONS;
   ```

2. Lanza los lotes al actor configurado (`APIFY_ACTOR_ID`).

3. Actualiza la base de datos con la información recibida.

4. Si un perfil devuelve error 403 u otro tipo de fallo, se marca automáticamente:
   ```sql
   UPDATE profiles
   SET public_identifier = 'INACCESIBLE'
   WHERE linkedin_url = '...';
   ```

De esta forma, dichos perfiles **no volverán a procesarse en ejecuciones futuras**.

---

## ⚙️ Parámetros configurables (.env)

El archivo `.env` en la raíz del proyecto debe contener todas las variables necesarias para la conexión y configuración:

```env
# --- PostgreSQL ---
PG_HOST=
PG_PORT=
PG_DB=
PG_USER=
PG_PASSWORD=
PG_SCHEMA=
PG_SSLMODE=

# --- Configuración general ---
CHUNK_SIZE=20                # Cantidad de perfiles por lote
MAX_URLS_PER_RUN=1000         # Límite total de perfiles por ejecución (0 = sin límite) (Coste = $ 4 por cada 1000 perfiles)
REFRESH_CHILDREN=true        # Actualiza experiencias, estudios, idiomas, etc.
MIN_CONNECTIONS=250          # Mínimo de conexiones requeridas para procesar el perfil

# --- Apify / HarvestAPI ---
APIFY_TOKEN=...
APIFY_ACTOR_ID=harvestapi~linkedin-profile-scraper
```

> 💡 Puedes ajustar `MIN_CONNECTIONS` según el filtro deseado.  
> Si quieres procesar todos los perfiles sin importar las conexiones, usa `MIN_CONNECTIONS=0`.

---

## 🧾 Ejemplo de salida en consola

Durante la ejecución verás mensajes como estos:

```
Encontrados 175 pendientes. CHUNK_SIZE=20
Lanzado run 7Ui5FYbl5gh0PKoag (urls=20)
⚠️ 3 perfiles marcados como INACCESIBLE.
🧾 Lote listo: 17 perfiles. Acumulado: 34
🎉 Terminado. Perfiles actualizados: 120
```

---

## 🧩 Script de inspección de perfiles (`inspect_profile_v2.py`)

Este script permite **verificar manualmente** la información completa de un perfil en la base de datos, ya sea usando su `profile_id` o su `linkedin_url`.  
Está pensado para comprobar que los datos extraídos por el orquestador coincidan con la información real visible en LinkedIn.

---

### ⚙️ Funcionalidad

- Muestra en consola todos los datos asociados a un perfil:
  - Información básica (`profiles`)
  - Experiencias laborales (`experiences` + `companies`)
  - Formación académica (`educations` + `educational_institutions`)
  - Idiomas (`profile_languages` + `languages`)
  - Habilidades (`profile_skills` + `skills`)
  - Resumen de cobertura por categoría

- Permite **exportar un JSON tipo “dossier”** con toda esa información ya normalizada.

---

### 🚀 Uso

Desde la raíz del proyecto:

```bash
# Por profile_id
python inspect_profile_v2.py --id 4067

# Por URL de LinkedIn (resuelve automáticamente el profile_id)
python inspect_profile_v2.py --url "https://www.linkedin.com/in/iñaki-garin-candido-1aa6441b7"

# Exportar los datos a un JSON
python inspect_profile_v2.py --id 4067 --out dossier_4067.json


## 🧹 Notas adicionales

- Los perfiles con `public_identifier = 'INACCESIBLE'` **no se volverán a procesar**.
- Para volver a incluir alguno manualmente:
  ```sql
  UPDATE linkedin.profiles
  SET public_identifier = NULL
  WHERE linkedin_url = 'https://www.linkedin.com/in/...';
  ```
- Si el campo `connections` no está presente o no se usa, basta con poner:
  ```
  MIN_CONNECTIONS=0
  ```

---

🧠 **Autor:** Equipo LATAM Connect  
📅 **Última actualización:** Octubre 2025  
📍 **Ubicación del script:** `src/orchestrate_from_db.py`
