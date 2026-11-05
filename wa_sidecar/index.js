/**
 * WhatsApp sidecar for tg-channel-joiner.
 *
 * Runs a Baileys (WhatsApp Web multi-device) session and exposes a tiny local
 * HTTP API that bot.py uses to upload downloaded Telegram files into WhatsApp groups.
 *
 * Endpoints (JSON unless noted, all bound to 127.0.0.1 by default):
 *   GET  /status          -> connection state + linked account
 *   GET  /qr              -> PNG of the current login QR code (404 if none pending)
 *   POST /pair  {phone}   -> request an 8-char pairing code (alternative to QR)
 *   GET  /groups          -> list of joined groups [{id, subject, size}]
 *   POST /send  {...}     -> upload a local file to a chat (see handleSend)
 *   POST /text  {jid,text}-> send a plain text message
 *   POST /logout          -> unlink this device and start a fresh login
 *
 * If WA_API_TOKEN is set, every request must carry header `x-api-key: <token>`.
 */
import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { exec } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import makeWASocket, {
    Browsers,
    DisconnectReason,
    fetchLatestBaileysVersion,
    useMultiFileAuthState,
} from 'baileys';
import pino from 'pino';
import QRCode from 'qrcode';
import sharp from 'sharp';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const HOST = process.env.WA_SIDECAR_HOST || '127.0.0.1';
const PORT = parseInt(process.env.WA_SIDECAR_PORT || '3100', 10);
const API_TOKEN = (process.env.WA_API_TOKEN || '').trim();
const AUTH_DIR = process.env.WA_AUTH_DIR || path.join(__dirname, 'auth');
const LOG_LEVEL = process.env.WA_LOG_LEVEL || 'warn';

const logger = pino({ level: LOG_LEVEL });

let sock = null;
let connection = 'connecting'; // connecting | qr | open | close
let currentQr = null;
let lastError = null;
let groupsCache = { at: 0, groups: [] };

function log(...args) {
    console.log('[wa-sidecar]', ...args);
}

async function startSocket() {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);

    let version;
    try {
        ({ version } = await fetchLatestBaileysVersion());
    } catch {
        version = undefined; // fall back to Baileys' bundled default
    }

    sock = makeWASocket({
        version,
        auth: state,
        logger,
        browser: Browsers.ubuntu('Chrome'),
        markOnlineOnConnect: false,
        syncFullHistory: false,
        // No timeout for large media uploads (up to 2 GB documents).
        mediaUploadTimeoutMs: 0,
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async (update) => {
        const { connection: conn, lastDisconnect, qr } = update;

        if (qr) {
            currentQr = qr;
            connection = 'qr';
            log('Scan this QR code with WhatsApp > Linked devices (or use /walogin in Telegram):');
            try {
                console.log(await QRCode.toString(qr, { type: 'terminal', small: true }));
            } catch {
                /* ignore */
            }
        }

        if (conn === 'open') {
            connection = 'open';
            currentQr = null;
            lastError = null;
            groupsCache = { at: 0, groups: [] };
            log(`Connected as ${sock.user?.id} (${sock.user?.name || 'no name'})`);
        }

        if (conn === 'close') {
            connection = 'close';
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            lastError = lastDisconnect?.error?.message || String(statusCode || 'unknown');
            const loggedOut = statusCode === DisconnectReason.loggedOut;
            log(`Connection closed (${lastError}). ${loggedOut ? 'Logged out - clearing session.' : 'Reconnecting...'}`);

            if (loggedOut) {
                fs.rmSync(AUTH_DIR, { recursive: true, force: true });
            }
            setTimeout(() => {
                startSocket().catch((err) => {
                    lastError = err.message;
                    log('Failed to restart socket:', err);
                });
            }, loggedOut ? 1000 : 3000);
        }
    });
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

function sendJson(res, status, body) {
    const data = JSON.stringify(body);
    res.writeHead(status, {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
    });
    res.end(data);
}

function readJsonBody(req) {
    return new Promise((resolve, reject) => {
        let raw = '';
        req.on('data', (chunk) => {
            raw += chunk;
            if (raw.length > 1024 * 1024) {
                reject(new Error('Request body too large'));
                req.destroy();
            }
        });
        req.on('end', () => {
            if (!raw) return resolve({});
            try {
                resolve(JSON.parse(raw));
            } catch {
                reject(new Error('Invalid JSON body'));
            }
        });
        req.on('error', reject);
    });
}

function requireOpen(res) {
    if (connection !== 'open' || !sock) {
        sendJson(res, 503, {
            error: 'WhatsApp is not connected',
            connection,
            hasQr: !!currentQr,
        });
        return false;
    }
    return true;
}

async function getGroups(force = false) {
    if (!force && groupsCache.groups.length && Date.now() - groupsCache.at < 60_000) {
        return groupsCache.groups;
    }
    const all = await sock.groupFetchAllParticipating();
    const groups = Object.values(all)
        .map((g) => ({
            id: g.id,
            subject: g.subject || '',
            size: g.size ?? (g.participants ? g.participants.length : 0),
            announce: !!g.announce,
        }))
        .sort((a, b) => a.subject.localeCompare(b.subject));
    groupsCache = { at: Date.now(), groups };
    return groups;
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/**
 * POST /send
 * {
 *   jid: "1203630xxxx@g.us",
 *   type: "video" | "image" | "audio" | "document",
 *   path: "/abs/path/to/file",
 *   caption?: string,
 *   fileName?: string,
 *   mimetype?: string,
 *   thumbPath?: string,      // optional JPEG thumbnail (image/video/document)
 *   seconds?: number, width?: number, height?: number
 * }
 */
async function handleSend(req, res) {
    if (!requireOpen(res)) return;
    const body = await readJsonBody(req);
    const { jid, type, caption, fileName, mimetype, thumbPath, seconds, width, height } = body;
    const filePath = body.path;

    if (!jid || !filePath || !type) {
        return sendJson(res, 400, { error: 'jid, path and type are required' });
    }
    if (!['video', 'image', 'audio', 'document'].includes(type)) {
        return sendJson(res, 400, { error: `Unsupported type: ${type}` });
    }
    if (!fs.existsSync(filePath)) {
        return sendJson(res, 400, { error: `File not found: ${filePath}` });
    }

    log(`POST /send: type=${type}, jid=${jid}, file=${path.basename(filePath)}, thumb=${thumbPath ? 'yes' : 'no'}`);

    const content = { [type]: { url: filePath } };
    if (mimetype) content.mimetype = mimetype;
    if (caption && type !== 'audio') content.caption = caption;
    if (type === 'document') content.fileName = fileName || path.basename(filePath);
    if (seconds && (type === 'video' || type === 'audio')) content.seconds = Math.round(seconds);
    if (width && height && (type === 'video' || type === 'image')) {
        content.width = width;
        content.height = height;
    }
    if (thumbPath && type !== 'audio' && fs.existsSync(thumbPath)) {
        try {
            // WhatsApp micro-thumbnails must be tiny (32-48px) to decode properly in mobile blur shaders
            const buf = await sharp(thumbPath)
                .resize({ width: 48, withoutEnlargement: true })
                .jpeg({ quality: 50 })
                .toBuffer();
            content.jpegThumbnail = buf.toString('base64');
        } catch {
            try {
                content.jpegThumbnail = fs.readFileSync(thumbPath).toString('base64');
            } catch {
                /* ignore thumbnail errors */
            }
        }
    }

    // Fallback: If sending a video and no thumbnail was provided, extract a clean frame at 1s
    if (type === 'video' && !content.jpegThumbnail && fs.existsSync(filePath)) {
        try {
            const tmpThumb = path.join(os.tmpdir(), `wa_thumb_${Date.now()}_${Math.random().toString(36).slice(2)}.jpg`);
            const seekTime = (seconds && seconds >= 2) ? '00:00:01' : '00:00:00';
            await new Promise((resolve, reject) => {
                exec(`ffmpeg -y -ss ${seekTime} -i "${filePath}" -vframes 1 -vf scale=64:-1 -q:v 4 "${tmpThumb}"`, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
            if (fs.existsSync(tmpThumb)) {
                try {
                    const buf = await sharp(tmpThumb)
                        .resize({ width: 48, withoutEnlargement: true })
                        .jpeg({ quality: 50 })
                        .toBuffer();
                    content.jpegThumbnail = buf.toString('base64');
                } catch {
                    content.jpegThumbnail = fs.readFileSync(tmpThumb).toString('base64');
                }
                fs.unlink(tmpThumb, () => {});
            }
        } catch {
            /* ignore fallback thumbnail errors */
        }
    }

    const started = Date.now();
    const sent = await sock.sendMessage(jid, content);
    log(`Sent ${type} to ${jid} in ${Date.now() - started}ms (id: ${sent?.key?.id || 'none'})`);
    sendJson(res, 200, {
        ok: true,
        id: sent?.key?.id || null,
        elapsedMs: Date.now() - started,
    });
}

async function handleText(req, res) {
    if (!requireOpen(res)) return;
    const { jid, text } = await readJsonBody(req);
    if (!jid || !text) return sendJson(res, 400, { error: 'jid and text are required' });
    log(`POST /text: jid=${jid}, text="${text.slice(0, 60).replace(/\n/g, ' ')}"`);
    const sent = await sock.sendMessage(jid, { text });
    sendJson(res, 200, { ok: true, id: sent?.key?.id || null });
}

async function handlePair(req, res) {
    const { phone } = await readJsonBody(req);
    const digits = String(phone || '').replace(/\D/g, '');
    if (!digits) return sendJson(res, 400, { error: 'phone (with country code) is required' });
    if (connection === 'open') return sendJson(res, 409, { error: 'Already connected' });
    if (!sock) return sendJson(res, 503, { error: 'Socket not ready yet' });
    const code = await sock.requestPairingCode(digits);
    sendJson(res, 200, { ok: true, code });
}

async function handleLogout(_req, res) {
    if (sock && connection === 'open') {
        await sock.logout(); // triggers connection.update -> loggedOut -> fresh QR
    } else {
        fs.rmSync(AUTH_DIR, { recursive: true, force: true });
        try {
            sock?.end(new Error('Manual logout'));
        } catch {
            /* ignore */
        }
    }
    sendJson(res, 200, { ok: true });
}

const server = http.createServer(async (req, res) => {
    try {
        if (API_TOKEN && req.headers['x-api-key'] !== API_TOKEN) {
            return sendJson(res, 401, { error: 'Unauthorized' });
        }
        const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
        const route = `${req.method} ${url.pathname}`;

        switch (route) {
            case 'GET /status':
                return sendJson(res, 200, {
                    connection,
                    hasQr: !!currentQr,
                    me: sock?.user ? { id: sock.user.id, name: sock.user.name || null } : null,
                    lastError,
                });
            case 'GET /qr': {
                if (!currentQr) return sendJson(res, 404, { error: 'No QR code pending', connection });
                const png = await QRCode.toBuffer(currentQr, { type: 'png', width: 512, margin: 2 });
                res.writeHead(200, { 'Content-Type': 'image/png', 'Content-Length': png.length });
                return res.end(png);
            }
            case 'GET /groups':
                if (!requireOpen(res)) return;
                return sendJson(res, 200, {
                    groups: await getGroups(url.searchParams.get('refresh') === '1'),
                });
            case 'POST /send':
                return await handleSend(req, res);
            case 'POST /text':
                return await handleText(req, res);
            case 'POST /pair':
                return await handlePair(req, res);
            case 'POST /logout':
                return await handleLogout(req, res);
            default:
                return sendJson(res, 404, { error: `No route for ${route}` });
        }
    } catch (err) {
        log('Request failed:', err);
        if (!res.headersSent) {
            sendJson(res, 500, { error: err?.message || String(err) });
        } else {
            res.end();
        }
    }
});

// Large uploads can take a long time; never time out the local request.
server.requestTimeout = 0;
server.headersTimeout = 0;
server.timeout = 0;

server.listen(PORT, HOST, () => {
    log(`HTTP API listening on http://${HOST}:${PORT}`);
});

startSocket().catch((err) => {
    lastError = err.message;
    log('Failed to start WhatsApp socket:', err);
});

for (const sig of ['SIGINT', 'SIGTERM']) {
    process.on(sig, () => {
        log(`Received ${sig}, shutting down.`);
        try {
            sock?.end(undefined);
        } catch {
            /* ignore */
        }
        server.close(() => process.exit(0));
        setTimeout(() => process.exit(0), 2000).unref();
    });
}
