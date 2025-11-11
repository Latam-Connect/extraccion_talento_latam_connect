#!/usr/bin/env python3
"""
generate_variants_vscode.py

Script para generar variantes de una URL (por ejemplo de LinkedIn)
y probar cuál fuerza un unfurl en Slack o en un navegador.

Uso:
  Simplemente ejecuta el archivo desde VS Code (F5 o ▶ Run)
  y pega la URL cuando te la pida.
"""

import time
import urllib.parse as up
import random
import string
from typing import List


def _rand_token(n=6) -> str:
    """Genera una cadena aleatoria alfanumérica."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def make_url_variants(url: str) -> List[str]:
    """Crea varias versiones de la misma URL con distintos parámetros."""
    ts = str(int(time.time() * 1000))
    parsed = up.urlparse(url)

    variants = []

    # Variante 1: _lc_probe
    q1 = up.parse_qsl(parsed.query, keep_blank_values=True)
    q1.append(("_lc_probe", ts))
    variants.append(parsed._replace(query=up.urlencode(q1)))

    # Variante 2: trk=public_profile_<timestamp>
    q2 = up.parse_qsl(parsed.query, keep_blank_values=True)
    q2.append(("trk", f"public_profile_{ts}"))
    variants.append(parsed._replace(query=up.urlencode(q2)))

    # Variante 3: originalSubdomain + t
    q3 = up.parse_qsl(parsed.query, keep_blank_values=True)
    q3.append(("originalSubdomain", "es"))
    q3.append(("t", ts))
    variants.append(parsed._replace(query=up.urlencode(q3)))

    # Variante 4: token aleatorio
    q4 = up.parse_qsl(parsed.query, keep_blank_values=True)
    q4.append(("r", _rand_token()))
    q4.append(("ts", ts))
    variants.append(parsed._replace(query=up.urlencode(q4)))

    # Variante 5: fragmento
    variants.append(parsed._replace(fragment=f"slack-{ts}"))

    return [up.urlunparse(v) for v in variants]


def main():
    print("🔗 Generador de variantes de URL para probar unfurls en Slack/LinkedIn\n")
    url = input("Pega la URL original: ").strip()

    if not url:
        print("⚠️ No has introducido ninguna URL.")
        return

    if not (url.startswith("http://") or url.startswith("https://")):
        print("⚠️ La URL debe comenzar con http:// o https://")
        return

    variants = make_url_variants(url)
    print("\nVariantes generadas:\n")
    for i, v in enumerate(variants, start=1):
        print(f"[{i}] {v}")

    print("\n✅ Copia una de estas y pégala en Slack para probar el unfurl.\n")


if __name__ == "__main__":
    main()
