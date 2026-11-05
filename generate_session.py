import asyncio
import os
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv()


async def main() -> None:
    api_id_str = os.getenv("API_ID") or input("Enter your API_ID: ").strip()
    api_hash = os.getenv("API_HASH") or input("Enter your API_HASH: ").strip()

    async with TelegramClient(StringSession(), int(api_id_str), api_hash) as client:
        session_str = client.session.save()
        print("\n✅ Your Telethon STRING_SESSION (copy into .env as STRING_SESSION=...):\n")
        print(session_str)
        print()


if __name__ == "__main__":
    asyncio.run(main())
