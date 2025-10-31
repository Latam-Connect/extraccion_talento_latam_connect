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
