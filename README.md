# Telegram Channel Joiner & File Downloader Bot

A hybrid Telegram bot built with **Telethon** where you interact with your **@BotFather** bot, and your connected **Telegram User Account** automatically:
1. **Joins channels** (public links, `@username`, or private `t.me/+...` invite links).
2. **Downloads & sends files/messages** from private or public channel message links (e.g. `https://t.me/c/4351613423/2`), including content-restricted (`noforwards`) channels.

## Features
- **Join Public & Private Channels**:
  - Public: `https://t.me/username`, `@username`
  - Private: `https://t.me/+InviteHash`, `https://t.me/joinchat/InviteHash`, `tg://join?invite=InviteHash`
- **Download & Send Files by Message Link**:
  - Private channel post links: `https://t.me/c/4351613423/2`
  - Topic/forum post links: `https://t.me/c/4351613423/10/25`
  - Batch message ranges: `https://t.me/c/4351613423/2-5` (downloads messages 2 through 5 sequentially)
  - Public channel post links: `https://t.me/channel_name/123`
  - Live **Download & Upload Progress Bars** with speed (`MB/s`), preserving original filenames, thumbnails, captions, and video streaming support.
- **Leave Channels**: Use `/leave <@username | link | channel_id>` to leave any joined channel.
- **Bulk Download → Telegram Channel**: `/tg <links or range>` downloads/copies posts (including from `noforwards` restricted channels) and uploads them to a destination Telegram channel/group.
- **Bulk Download → WhatsApp Group**: `/wa <links or range>` downloads every post and uploads it to a WhatsApp group (Paid VIP feature).
- **Auto-Sync Channels → Telegram Channel or WhatsApp**: `/sync` & `/tgsync` continuously listen for new posts in monitored Telegram channels and automatically sync them to another Telegram channel and/or WhatsApp group in real-time.
- **Interactive Menu & Public / Paid VIP Tier System**:
  - **Interactive `/start` & `/help` Dashboard**: Step-by-step visual guides and inline buttons for every feature.
  - **🆓 Free Plan (All Users)**: Unlimited channel joins, single/bulk downloads, video streaming (`/stream`), Telegram channel upload (`/tg`), and **1 Free Telegram-to-Telegram Auto-Sync rule** (`/tgsync`).
  - **💎 Paid VIP Plan**: Unlocks **WhatsApp Group Auto-Sync & Bulk Upload** (`/sync` & `/wa`) and **Multiple Telegram Auto-Sync rules**. Users can pay 50 Telegram Stars for one calendar month (`/myplan`); owner-granted VIP access remains available (`/addpaid`, `/removepaid`, `/paidusers`).

## Paid VIP Payments (Telegram Stars)

The upgrade screen offers a one-time **50 Telegram Stars (XTR)** invoice for one calendar month of VIP access. Users can review `/terms` and must explicitly agree before checkout. Access starts after Telegram confirms successful payment; renewals extend the current expiration date, and the customer must renew manually (no recurring charge). Payment records and expirations are stored in `payments.sqlite3` beside `bot.py`; legacy owner/manual VIP grants remain unchanged. `/paysupport` directs users to the configured owner contact for billing and refund requests; Telegram Support cannot resolve purchases made through this bot. Owners can refund a user's latest unrefunded Stars payment with `/refundstars <user_id>` (or specify a charge ID).

This checkout uses the existing `BOT_TOKEN` and Telegram Bot API; no third-party provider token is required. The payment worker uses Bot API long polling (`getUpdates`), so the token must not also be running a webhook or a second `getUpdates` consumer. Configure `OWNER_CONTACT` in `.env` so users have a clear support contact. Test the flow with a test bot before offering paid access.

## Setup

### 1. Get Telegram Credentials
1. **API_ID & API_HASH**: Go to [my.telegram.org](https://my.telegram.org) -> **API development tools** and copy your `api_id` and `api_hash`.
2. **BOT_TOKEN**: Message [@BotFather](https://t.me/BotFather) on Telegram, run `/newbot`, and copy your bot token.

### 2. Install Dependencies
```bash
cd /home/tharusha/Documents/tg-channel-joiner
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure `.env`
```bash
cp .env.example .env
```
Edit `.env` and fill in your `API_ID`, `API_HASH`, and `BOT_TOKEN`.

### 4. Run the Bot
```bash
python bot.py
```
1. First send the channel invite link (e.g. `https://t.me/+InviteHash`) to the bot so your user account joins the channel.
2. Then send any file/message link from that channel (e.g. `https://t.me/c/4351613423/2`) and the bot will download and send the file directly to you!


## WhatsApp Group Upload (`/wa`)

`bot.py` starts a small Node.js sidecar (`wa_sidecar/`, built on [Baileys](https://github.com/WhiskeySockets/Baileys)), which acts as a linked WhatsApp device. Files are downloaded with your Telegram user account and then uploaded to the WhatsApp group.

### Setup (one time)
```bash
cd wa_sidecar && npm install   # requires Node.js >= 20
```
Restart the bot, then in Telegram:
1. `/walogin` — the bot sends a QR code. Scan it from WhatsApp → **Linked devices** → **Link a device**.
   (Or use `/walogin 94771234567` to get a pairing code instead.)
2. `/wagroups` — list your groups and their IDs.
3. `/wagroup <group_id | name>` — set the default group (saved to `.env` as `WA_GROUP_ID`).

### Usage
| Command | Description |
|---|---|
| `/wa https://t.me/c/4351613423/2-50` | Upload posts 2–50 to the default group |
| `/wa https://t.me/c/4351613423/2 https://t.me/c/4351613423/50` | Start → end range |
| `/wa My Group https://t.me/c/4351613423/2-10` | Upload to a specific group (name or `…@g.us` ID) |
| Reply `/wa` to a message with links | Use the links from that message |
| `/cancel` | Stop the running upload |
| `/walogout` | Unlink the WhatsApp device |

How files are sent:
- MP4 videos up to `WA_VIDEO_MAX_MB` (default 100 MB) are sent as **videos**, JPEG/PNG up to 16 MB as **images**, and common audio formats as **audio**.
- Everything else, or anything over those limits, is sent as a **document**, up to `WA_MAX_FILE_MB` (default 2000 MB). Bigger files are skipped and listed in the summary.
- Captions are copied over, and text-only posts are sent as text messages.

> ⚠️ This uses an unofficial WhatsApp Web client. Sending a lot of messages quickly can get a number banned, so keep `WA_SEND_DELAY_SECONDS` / `BULK_COOLDOWN_*` reasonable, and ideally use a secondary number.


## Telegram Channel Upload (`/tg`)

Bulk copy or download posts from any public or private Telegram channel (including `noforwards` content-restricted channels) and upload them directly to your own Telegram channel or group.

### Commands
| Command | Description |
|---|---|
| `/tgchannels [query]` | List your Telegram channels/groups and their IDs |
| `/tgchannel <@username \| channel_id \| name>` | Set the default destination Telegram channel (saved to `.env` as `TG_UPLOAD_CHANNEL_ID`) |
| `/tg https://t.me/c/4351613423/2-50` | Upload posts 2–50 to the default Telegram channel |
| `/tg https://t.me/c/4351613423/2 https://t.me/c/4351613423/50` | Start → end range |
| `/tg @mychannel https://t.me/c/4351613423/2-10` | Upload to a specific Telegram channel (`@username`, `-100...` ID, link, or title) |
| Reply `/tg [channel]` to any file/video/message | Upload that file or message from the chat to the destination Telegram channel |
| `/cancel` | Stop the running bulk upload |


## Auto-Sync: Telegram ➔ Telegram Channel & WhatsApp (`/sync` & `/tgsync`)

Automatically forward/upload new messages, videos, documents, photos, audio, and text posts from any Telegram channel directly to another **Telegram channel/group** or **WhatsApp group** in real-time as soon as they are published.

### Features
- **Real-Time Monitoring + Background Poller**: Captures new posts via Telethon's live event listener plus a periodic poller for 100% reliability.
- **Supports Both Telegram & WhatsApp Destinations**: Sync any source channel to another Telegram channel (`✈️`), a WhatsApp group (`📲`), or both simultaneously!
- **Works with `noforwards` Restricted Channels**: Automatically uses instant server-side copy when allowed, or parallel multi-connection download + `+faststart` MP4 remux + thumbnail extraction + parallel upload when content protection is enabled.
- **Persistent Configuration**: Saved to `sync_config.json` so sync mappings survive restarts.
- **Catchup / Backfill**: Sync the last N posts from a channel immediately with `/sync catchup`.
- **Interactive Inline Buttons**: Manage rules, pause/resume, toggle captions/text, select default Telegram channels, view WhatsApp groups, or run catchups directly from inline buttons.

### Commands
| Command | Description |
|---|---|
| `/sync` or `/tgsync` | View auto-sync dashboard, connection status, and interactive controls |
| `/tgsync add <src_channel> [dest_tg_channel]` | Auto-sync a Telegram channel to another Telegram channel (defaults to `TG_UPLOAD_CHANNEL_ID`) |
| `/sync addtg <src_channel> [dest_tg_channel]` | Alias to add a Telegram ➔ Telegram sync rule |
| `/sync add <tg_channel> [wa_group]` | Map a Telegram channel to a WhatsApp group (defaults to `WA_GROUP_ID`) |
| `/sync list` (or `/synclist`, `/tgsynclist`) | List all configured sync rules, IDs, and stats |
| `/sync pause <id>` | Temporarily pause auto-sync for a rule |
| `/sync resume <id>` | Resume a paused sync rule |
| `/sync remove <id>` (or `/unsync <id>`) | Stop auto-syncing and delete the rule |
| `/sync catchup <id> [count]` | Immediately sync the last N posts (default 5, max 30) |
| `/sync text <id> <on\|off>` | Toggle syncing text-only messages |
| `/sync caption <id> <on\|off>` | Toggle including media captions |
| `/sync notify <on\|off>` | Toggle Telegram status alerts when posts are synced |
| `/sync status` | Check queue length, sidecar status, and worker health |

### Examples
- `/tgsync add @source_news @my_backup_channel` — Auto-sync `@source_news` to `@my_backup_channel`
- `/tgsync add https://t.me/+AbCdEf -1001234567890` — Join private channel and auto-sync to destination Telegram channel
- `/sync add @technews` — Sync `@technews` to default WhatsApp group
- `/sync add https://t.me/+AbCdEf "My Group"` — Join private channel and sync to WhatsApp group "My Group"
- `/sync catchup 1 10` — Sync the last 10 messages from rule #1 right now
- `/sync pause 1` / `/sync resume 1` — Pause and resume rule #1
