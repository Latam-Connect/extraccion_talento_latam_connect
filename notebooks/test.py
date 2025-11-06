import ollama
import json
import re

prompt_base = """Analiza el siguiente texto y devuelve SOLO un JSON con las claves:

- "profesion": título profesional principal de la persona, claro y conciso, pero que conserve su significado 
  (por ejemplo: "Director de ventas", "Analista de datos", "Diseñador industrial", "Consultor", "Ingeniero de software", "Abogado").
  Si el texto describe varios roles o áreas, elige el más representativo y exprésalo de forma general, sin detalles innecesarios.

- "sector": categoría profesional general o combinada cuando sea relevante 
  (por ejemplo: "Tecnología / Salud", "Consultoría / Finanzas", "ONG / Educación", "Marketing / Publicidad"), en español.
  Si el texto se refiere a cooperación, políticas sociales o desarrollo humano, clasifica el sector como "ONG / cooperación internacional".
  Si no hay información clara sobre el sector, **infiera la categoría más probable a partir del rol profesional.**
  Por ejemplo, "Business Analyst" suele pertenecer al sector "Consultoría" o "Finanzas".

- "es_tech": true si pertenece al ámbito tecnológico, digital, IA, software o datos; false en caso contrario.

- "contactos_linkedin": número entero. 
  Si el texto contiene expresiones como "más de 500", "500+", "more than 500" u "over 500", asigna el valor 500. 
  Si no aparece información de contactos, asigna null.

  Si el texto no contiene información profesional, laboral o educativa, responde con:
{
  "profesion": null,
  "sector": null,
  "es_tech": null,
  "contactos_linkedin": valor detectado o null
}

El texto puede estar en cualquier idioma, pero las respuestas deben estar en español.
NO expliques nada. NO añadas texto fuera del JSON. Responde solo con JSON válido.
"""

texto = """Ubicación: Valencia/València · Más de 500 contactos en LinkedIn. Ver el perfil de Ignacio Javier Fernández Marcolongo en LinkedIn, una red profesional de más de 1.000 millones de miembros."""

resp = ollama.chat(
    model='phi3:3.8b',
    messages=[
        {"role": "system", "content": prompt_base},
        {"role": "user", "content": f"Texto: {texto}"}
    ]
)

raw = resp["message"]["content"]
# intentamos extraer el primer bloque { ... }
m = re.search(r'\{.*\}', raw, re.DOTALL)
if not m:
    print("⚠️ No encontré JSON en la respuesta:")
    print(raw)
else:
    json_str = m.group(0)
    try:
        data = json.loads(json_str)
        print(json.dumps(data, indent=2, ensure_ascii=False))
    except json.JSONDecodeError:
        print("⚠️ El modelo devolvió algo casi JSON pero no del todo:")
        print(json_str)
