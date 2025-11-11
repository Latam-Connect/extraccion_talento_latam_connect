import os
import time
from pathlib import Path

from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# carga el .env desde la raíz del proyecto
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

SLACK_BOT_TOKEN = os.getenv("SLACK_ACCESS_TOKEN") or os.getenv("SLACK_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

if not SLACK_BOT_TOKEN or not SLACK_CHANNEL_ID:
    raise RuntimeError("Falta SLACK_BOT_TOKEN o SLACK_CHANNEL_ID en el .env")

client = WebClient(token=SLACK_BOT_TOKEN)


def delete_messages_from_channel(channel_id: str):
    cursor = None
    total_deleted = 0

    while True:
        try:
            resp = client.conversations_history(
                channel=channel_id,
                limit=200,
                cursor=cursor
            )
        except SlackApiError as e:
            print(f"❌ Error al leer el canal: {e.response.get('error')}")
            break

        messages = resp.get("messages", [])
        if not messages:
            break

        for msg in messages:
            ts = msg.get("ts")
            # opcional: borrar solo los del bot
            # if msg.get("bot_id") is None:
            #     continue

            try:
                client.chat_delete(channel=channel_id, ts=ts)
                total_deleted += 1
                print(f"🗑️  Borrado mensaje ts={ts}")
            except SlackApiError as e:
                # puede ser not_in_channel, cant_delete_message, etc.
                print(f"⚠️ No se pudo borrar ts={ts}: {e.response.get('error')}")
            except Exception as e:
                print(f"⚠️ Error de red borrando ts={ts}: {e}")

            # pequeña pausa para no cabrear a slack
            time.sleep(0.4)

        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    print(f"✅ Terminado. Mensajes borrados: {total_deleted}")


if __name__ == "__main__":
    delete_messages_from_channel(SLACK_CHANNEL_ID)
