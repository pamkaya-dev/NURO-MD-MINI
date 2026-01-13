const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const { exec } = require('child_process');
const router = express.Router();
const pino = require('pino');
const moment = require('moment-timezone');
const Jimp = require('jimp');
const crypto = require('crypto');
const axios = require('axios');
const FileType = require('file-type');
const fetch = require('node-fetch');
const { MongoClient } = require('mongodb');

const {
  default: makeWASocket,
  useMultiFileAuthState,
  delay,
  getContentType,
  makeCacheableSignalKeyStore,
  Browsers,
  jidNormalizedUser,
  downloadContentFromMessage,
  proto,
  DisconnectReason
} = require('baileys');

// ---------------- PERFORMANCE OPTIMIZATIONS ----------------
const PQueue = require('p-queue');

// Create queues for different types of operations
const heavyCommandQueue = new PQueue({
  concurrency: 2,
  timeout: 30000
});

const mediaDownloadQueue = new PQueue({
  concurrency: 3,
  timeout: 15000
});

const databaseQueue = new PQueue({
  concurrency: 5,
  timeout: 10000
});

// Performance monitoring
const commandPerformance = new Map();
const performanceLog = fs.createWriteStream('performance.log', { flags: 'a' });

function logPerformance(message) {
  const timestamp = new Date().toISOString();
  performanceLog.write(`${timestamp} - ${message}\n`);
}

function trackCommandPerformance(command, startTime) {
  const duration = Date.now() - startTime;
  
  if (!commandPerformance.has(command)) {
    commandPerformance.set(command, {
      count: 0,
      totalTime: 0,
      maxTime: 0
    });
  }
  
  const stats = commandPerformance.get(command);
  stats.count++;
  stats.totalTime += duration;
  stats.maxTime = Math.max(stats.maxTime, duration);
  
  if (duration > 3000) {
    logPerformance(`⚠️ Slow command: ${command} took ${duration}ms`);
  }
}

// Memory cache with TTL
const configCache = new Map();
const CACHE_TTL = 60000; // 1 minute
const commandCooldown = new Map();
const COOLDOWN_TIME = 1000; // 1 second

// ---------------- CONFIG ----------------
const BOT_NAME_FANCY = 'NURO MD V1';

const config = {
  AUTO_VIEW_STATUS: 'true',
  AUTO_LIKE_STATUS: 'true',
  AUTO_RECORDING: 'false',
  AUTO_LIKE_EMOJI: ['☘️','💗','🫂','🙈','🍁','🙃','🧸','😘','🏴‍☠️','👀','❤️‍🔥'],
  PREFIX: '.',
  MAX_RETRIES: 3,
  GROUP_INVITE_LINK: 'https://chat.whatsapp.com/Ih0PCRnllMO2IEGFW5eV4n',
  RCD_IMAGE_PATH: 'https://files.catbox.moe/paap2h.jpg',
  NEWSLETTER_JID: '120363403935705046@newsletter',
  OTP_EXPIRY: 300000,
  WORK_TYPE: 'public',
  OWNER_NUMBER: process.env.OWNER_NUMBER || '94721017862',
  CHANNEL_LINK: 'https://whatsapp.com/channel/0029Vb6d1x73bbVBh3ibyx02',
  BOT_NAME: 'NURO MD',
  BOT_VERSION: '1.0.0V',
  OWNER_NAME: 'Tharaka Dilshan',
  IMAGE_PATH: 'https://files.catbox.moe/paap2h.jpg',
  BOT_FOOTER: '> *© 𝙿𝙾𝚆𝙴𝚁𝙴𝙳 𝙱𝚈 𝙽𝚄𝚁𝙾 〽️𝙳 ㋛*',
  BUTTON_IMAGES: { ALIVE: 'https://files.catbox.moe/paap2h.jpg' }
};

// ---------------- OPTIMIZED MONGO SETUP ----------------
const MONGO_URI = process.env.MONGO_URI || 'mongodb+srv://nuro-md-mini:nuro1234@cluster0.6z8bjhp.mongodb.net//';
const MONGO_DB = process.env.MONGO_DB || 'NURO_LOVE';

let mongoClientInstance = null;
let mongoDBInstance = null;
let sessionsCol, numbersCol, adminsCol, newsletterCol, configsCol, newsletterReactsCol;

async function getMongoDB() {
    if (mongoDBInstance && mongoClientInstance && mongoClientInstance.topology && mongoClientInstance.topology.isConnected()) {
        return mongoDBInstance;
    }
    
    try {
        mongoClientInstance = new MongoClient(MONGO_URI, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
            maxPoolSize: 10,
            minPoolSize: 1,
            socketTimeoutMS: 30000,
            connectTimeoutMS: 30000,
            serverSelectionTimeoutMS: 30000
        });
        
        await mongoClientInstance.connect();
        mongoDBInstance = mongoClientInstance.db(MONGO_DB);
        
        // Initialize collections
        sessionsCol = mongoDBInstance.collection('sessions');
        numbersCol = mongoDBInstance.collection('numbers');
        adminsCol = mongoDBInstance.collection('admins');
        newsletterCol = mongoDBInstance.collection('newsletter_list');
        configsCol = mongoDBInstance.collection('configs');
        newsletterReactsCol = mongoDBInstance.collection('newsletter_reacts');
        
        // Create indexes if they don't exist
        await Promise.all([
            sessionsCol.createIndex({ number: 1 }, { unique: true }),
            numbersCol.createIndex({ number: 1 }, { unique: true }),
            newsletterCol.createIndex({ jid: 1 }, { unique: true }),
            newsletterReactsCol.createIndex({ jid: 1 }, { unique: true }),
            configsCol.createIndex({ number: 1 }, { unique: true })
        ]);
        
        console.log('✅ Mongo initialized with optimized connection');
        return mongoDBInstance;
    } catch (error) {
        console.error('MongoDB connection error:', error);
        throw error;
    }
}

// ---------------- OPTIMIZED Mongo helpers ----------------
async function saveCredsToMongo(number, creds, keys = null) {
  return databaseQueue.add(async () => {
    try {
      const db = await getMongoDB();
      const sanitized = number.replace(/[^0-9]/g, '');
      const doc = { 
        number: sanitized, 
        creds, 
        keys, 
        updatedAt: new Date(),
        lastUpdate: Date.now()
      };
      await sessionsCol.updateOne(
        { number: sanitized }, 
        { $set: doc }, 
        { upsert: true }
      );
    } catch (e) { 
      console.error('saveCredsToMongo error:', e); 
    }
  });
}

async function loadCredsFromMongo(number) {
  return databaseQueue.add(async () => {
    try {
      const sanitized = number.replace(/[^0-9]/g, '');
      const doc = await sessionsCol.findOne({ number: sanitized });
      return doc || null;
    } catch (e) { 
      console.error('loadCredsFromMongo error:', e); 
      return null; 
    }
  });
}

async function removeSessionFromMongo(number) {
  return databaseQueue.add(async () => {
    try {
      const sanitized = number.replace(/[^0-9]/g, '');
      await sessionsCol.deleteOne({ number: sanitized });
    } catch (e) { 
      console.error('removeSessionToMongo error:', e); 
    }
  });
}

async function addNumberToMongo(number) {
  return databaseQueue.add(async () => {
    try {
      const sanitized = number.replace(/[^0-9]/g, '');
      await numbersCol.updateOne(
        { number: sanitized }, 
        { $set: { number: sanitized, addedAt: new Date() } }, 
        { upsert: true }
      );
    } catch (e) { 
      console.error('addNumberToMongo', e); 
    }
  });
}

async function removeNumberFromMongo(number) {
  return databaseQueue.add(async () => {
    try {
      const sanitized = number.replace(/[^0-9]/g, '');
      await numbersCol.deleteOne({ number: sanitized });
    } catch (e) { 
      console.error('removeNumberFromMongo', e); 
    }
  });
}

async function getAllNumbersFromMongo() {
  return databaseQueue.add(async () => {
    try {
      const docs = await numbersCol.find({}, { projection: { number: 1 } }).toArray();
      return docs.map(d => d.number);
    } catch (e) { 
      console.error('getAllNumbersFromMongo', e); 
      return []; 
    }
  });
}

async function loadAdminsFromMongo() {
  return databaseQueue.add(async () => {
    try {
      const docs = await adminsCol.find({}, { projection: { jid: 1 } }).toArray();
      return docs.map(d => d.jid || d.number).filter(Boolean);
    } catch (e) { 
      console.error('loadAdminsFromMongo', e); 
      return []; 
    }
  });
}

async function addAdminToMongo(jidOrNumber) {
  return databaseQueue.add(async () => {
    try {
      const doc = { jid: jidOrNumber, addedAt: new Date() };
      await adminsCol.updateOne(
        { jid: jidOrNumber }, 
        { $set: doc }, 
        { upsert: true }
      );
    } catch (e) { 
      console.error('addAdminToMongo', e); 
    }
  });
}

async function removeAdminFromMongo(jidOrNumber) {
  return databaseQueue.add(async () => {
    try {
      await adminsCol.deleteOne({ jid: jidOrNumber });
    } catch (e) { 
      console.error('removeAdminFromMongo', e); 
    }
  });
}

async function addNewsletterToMongo(jid, emojis = []) {
  return databaseQueue.add(async () => {
    try {
      const doc = { 
        jid, 
        emojis: Array.isArray(emojis) ? emojis : [], 
        addedAt: new Date() 
      };
      await newsletterCol.updateOne(
        { jid }, 
        { $set: doc }, 
        { upsert: true }
      );
    } catch (e) { 
      console.error('addNewsletterToMongo', e); 
      throw e; 
    }
  });
}

async function removeNewsletterFromMongo(jid) {
  return databaseQueue.add(async () => {
    try {
      await newsletterCol.deleteOne({ jid });
    } catch (e) { 
      console.error('removeNewsletterFromMongo', e); 
      throw e; 
    }
  });
}

async function listNewslettersFromMongo() {
  return databaseQueue.add(async () => {
    try {
      const docs = await newsletterCol.find({}).toArray();
      return docs.map(d => ({ 
        jid: d.jid, 
        emojis: Array.isArray(d.emojis) ? d.emojis : [] 
      }));
    } catch (e) { 
      console.error('listNewslettersFromMongo', e); 
      return []; 
    }
  });
}

async function saveNewsletterReaction(jid, messageId, emoji, sessionNumber) {
  return databaseQueue.add(async () => {
    try {
      const doc = { 
        jid, 
        messageId, 
        emoji, 
        sessionNumber, 
        ts: new Date() 
      };
      const db = await getMongoDB();
      const col = db.collection('newsletter_reactions_log');
      await col.insertOne(doc);
    } catch (e) { 
      console.error('saveNewsletterReaction', e); 
    }
  });
}

// Cached version of setUserConfigInMongo
async function setUserConfigInMongo(number, conf) {
  return databaseQueue.add(async () => {
    try {
      const sanitized = number.replace(/[^0-9]/g, '');
      await configsCol.updateOne(
        { number: sanitized }, 
        { $set: { 
          number: sanitized, 
          config: conf, 
          updatedAt: new Date() 
        }}, 
        { upsert: true }
      );
      
      // Update cache
      const cacheKey = `config_${sanitized}`;
      configCache.set(cacheKey, {
        data: conf,
        timestamp: Date.now()
      });
    } catch (e) { 
      console.error('setUserConfigInMongo', e); 
    }
  });
}

// Cached version of loadUserConfigFromMongo
async function loadUserConfigFromMongo(number) {
  return databaseQueue.add(async () => {
    try {
      const sanitized = number.replace(/[^0-9]/g, '');
      const cacheKey = `config_${sanitized}`;
      
      // Check cache first
      const cached = configCache.get(cacheKey);
      if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
        return cached.data;
      }
      
      const doc = await configsCol.findOne({ number: sanitized });
      const result = doc ? doc.config : null;
      
      // Update cache
      if (result) {
        configCache.set(cacheKey, {
          data: result,
          timestamp: Date.now()
        });
      }
      
      return result;
    } catch (e) { 
      console.error('loadUserConfigFromMongo', e); 
      return null; 
    }
  });
}

async function addNewsletterReactConfig(jid, emojis = []) {
  return databaseQueue.add(async () => {
    try {
      await newsletterReactsCol.updateOne(
        { jid }, 
        { $set: { jid, emojis, addedAt: new Date() } }, 
        { upsert: true }
      );
    } catch (e) { 
      console.error('addNewsletterReactConfig', e); 
      throw e; 
    }
  });
}

async function removeNewsletterReactConfig(jid) {
  return databaseQueue.add(async () => {
    try {
      await newsletterReactsCol.deleteOne({ jid });
    } catch (e) { 
      console.error('removeNewsletterReactConfig', e); 
      throw e; 
    }
  });
}

async function listNewsletterReactsFromMongo() {
  return databaseQueue.add(async () => {
    try {
      const docs = await newsletterReactsCol.find({}).toArray();
      return docs.map(d => ({ 
        jid: d.jid, 
        emojis: Array.isArray(d.emojis) ? d.emojis : [] 
      }));
    } catch (e) { 
      console.error('listNewsletterReactsFromMongo', e); 
      return []; 
    }
  });
}

async function getReactConfigForJid(jid) {
  return databaseQueue.add(async () => {
    try {
      const doc = await newsletterReactsCol.findOne({ jid });
      return doc ? (Array.isArray(doc.emojis) ? doc.emojis : []) : null;
    } catch (e) { 
      console.error('getReactConfigForJid', e); 
      return null; 
    }
  });
}

// ---------------- OPTIMIZED basic utils ----------------
function formatMessage(title, content, footer) {
  return `*${title}*\n\n${content}\n\n> *${footer}*`;
}

function generateOTP() { 
  return Math.floor(100000 + Math.random() * 900000).toString(); 
}

function getSriLankaTimestamp() { 
  return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); 
}

const activeSockets = new Map();
const socketCreationTime = new Map();
const otpStore = new Map();

// ---------------- OPTIMIZED helpers ----------------
async function joinGroup(socket) {
  let retries = config.MAX_RETRIES;
  const inviteCodeMatch = (config.GROUP_INVITE_LINK || '').match(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/);
  if (!inviteCodeMatch) return { status: 'failed', error: 'No group invite configured' };
  
  const inviteCode = inviteCodeMatch[1];
  while (retries > 0) {
    try {
      const response = await socket.groupAcceptInvite(inviteCode);
      if (response?.gid) return { status: 'success', gid: response.gid };
      throw new Error('No group ID in response');
    } catch (error) {
      retries--;
      let errorMessage = error.message || 'Unknown error';
      if (error.message && error.message.includes('not-authorized')) errorMessage = 'Bot not authorized';
      else if (error.message && error.message.includes('conflict')) errorMessage = 'Already a member';
      else if (error.message && error.message.includes('gone')) errorMessage = 'Invite invalid/expired';
      if (retries === 0) return { status: 'failed', error: errorMessage };
      await delay(2000 * (config.MAX_RETRIES - retries));
    }
  }
  return { status: 'failed', error: 'Max retries reached' };
}

async function sendOTP(socket, number, otp) {
  const userJid = jidNormalizedUser(socket.user.id);
  const message = formatMessage(
    `*🔐 𝐎𝚃𝙿 𝐕𝙴𝚁𝙸𝙵𝙸𝙲𝙰𝚃𝙸𝙾𝙽 — ${BOT_NAME_FANCY}*`, 
    `*𝐘𝙾𝚄𝚁 𝐎𝚃𝙿 𝐅𝙾𝚁 𝐂𝙾𝙽𝙵𝙸𝙶 𝐔𝙿𝙳𝙰𝚃𝙴 𝐈𝚂:* *${otp}*\n𝐓𝙷𝙸𝚂 𝐎𝚃𝙿 𝐖𝙸𝙻𝙻 𝐄𝚇𝙿𝙸𝚁𝙴 𝐈𝙽 5 𝐌𝙸𝙽𝚄𝚃𝙴𝚂.\n\n*𝐍𝚄𝙼𝙱𝙴𝚁:* ${number}`, 
    BOT_NAME_FANCY
  );
  
  try { 
    await socket.sendMessage(userJid, { text: message }); 
  } catch (error) { 
    console.error(`Failed to send OTP to ${number}:`, error); 
    throw error; 
  }
}

// ---------------- OPTIMIZED media download ----------------
async function downloadQuotedMedia(quoted, timeout = 10000) {
  return mediaDownloadQueue.add(async () => {
    if (!quoted) return null;
    
    const qTypes = ['imageMessage','videoMessage','audioMessage','documentMessage','stickerMessage'];
    const qType = qTypes.find(t => quoted[t]);
    if (!qType) return null;
    
    const messageType = qType.replace(/Message$/i, '').toLowerCase();
    
    try {
      // Add timeout to download
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('Download timeout')), timeout);
      });
      
      const stream = await Promise.race([
        downloadContentFromMessage(quoted[qType], messageType),
        timeoutPromise
      ]);
      
      let buffer = Buffer.from([]);
      for await (const chunk of stream) {
        buffer = Buffer.concat([buffer, chunk]);
        // Limit buffer size to prevent memory issues
        if (buffer.length > 50 * 1024 * 1024) {
          throw new Error('File too large');
        }
      }
      
      return {
        buffer,
        mime: quoted[qType].mimetype || '',
        caption: quoted[qType].caption || quoted[qType].fileName || '',
        ptt: quoted[qType].ptt || false,
        fileName: quoted[qType].fileName || ''
      };
    } catch (error) {
      console.error('Media download error:', error);
      return null;
    }
  });
}

// ---------------- OPTIMIZED handlers ----------------
async function setupNewsletterHandlers(socket, sessionNumber) {
  const rrPointers = new Map();

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key) return;
    const jid = message.key.remoteJid;

    try {
      const [followedDocs, reactConfigs] = await Promise.all([
        listNewslettersFromMongo(),
        listNewsletterReactsFromMongo()
      ]);
      
      const followedJids = followedDocs.map(d => d.jid);
      const reactMap = new Map();
      
      for (const r of reactConfigs) reactMap.set(r.jid, r.emojis || []);
      if (!followedJids.includes(jid) && !reactMap.has(jid)) return;

      let emojis = reactMap.get(jid) || null;
      if ((!emojis || emojis.length === 0) && followedDocs.find(d => d.jid === jid)) {
        emojis = (followedDocs.find(d => d.jid === jid).emojis || []);
      }
      if (!emojis || emojis.length === 0) emojis = config.AUTO_LIKE_EMOJI;

      let idx = rrPointers.get(jid) || 0;
      const emoji = emojis[idx % emojis.length];
      rrPointers.set(jid, (idx + 1) % emojis.length);

      const messageId = message.newsletterServerId || message.key.id;
      if (!messageId) return;

      let retries = 3;
      while (retries-- > 0) {
        try {
          if (typeof socket.newsletterReactMessage === 'function') {
            await socket.newsletterReactMessage(jid, messageId.toString(), emoji);
          } else {
            await socket.sendMessage(jid, { react: { text: emoji, key: message.key } });
          }
          
          // Save reaction in background without waiting
          saveNewsletterReaction(jid, messageId.toString(), emoji, sessionNumber || null)
            .catch(e => console.error('Failed to save reaction:', e));
          
          break;
        } catch (err) {
          console.warn(`Reaction attempt failed (${3 - retries}/3):`, err?.message || err);
          await delay(1200);
        }
      }
    } catch (error) {
      console.error('Newsletter reaction handler error:', error?.message || error);
    }
  });
}

async function setupStatusHandlers(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key || message.key.remoteJid !== 'status@broadcast' || !message.key.participant) return;
    
    try {
      // Load user config in parallel
      const userConfigPromise = sessionNumber ? loadUserConfigFromMongo(sessionNumber) : Promise.resolve({});
      const userConfig = await userConfigPromise;
      
      const userEmojis = userConfig.AUTO_LIKE_EMOJI || config.AUTO_LIKE_EMOJI;
      const autoViewStatus = userConfig.AUTO_VIEW_STATUS || config.AUTO_VIEW_STATUS;
      const autoLikeStatus = userConfig.AUTO_LIKE_STATUS || config.AUTO_LIKE_STATUS;
      const autoRecording = userConfig.AUTO_RECORDING || config.AUTO_RECORDING;

      // Execute actions in parallel where possible
      const actions = [];
      
      if (autoRecording === 'true') {
        actions.push(socket.sendPresenceUpdate("recording", message.key.remoteJid));
      }
      
      if (autoViewStatus === 'true') {
        actions.push((async () => {
          let retries = config.MAX_RETRIES;
          while (retries > 0) {
            try { 
              await socket.readMessages([message.key]); 
              break; 
            } catch (error) { 
              retries--; 
              await delay(1000 * (config.MAX_RETRIES - retries)); 
              if (retries===0) throw error; 
            }
          }
        })());
      }
      
      if (autoLikeStatus === 'true') {
        actions.push((async () => {
          const randomEmoji = userEmojis[Math.floor(Math.random() * userEmojis.length)];
          let retries = config.MAX_RETRIES;
          while (retries > 0) {
            try {
              await socket.sendMessage(message.key.remoteJid, { 
                react: { text: randomEmoji, key: message.key } 
              }, { statusJidList: [message.key.participant] });
              break;
            } catch (error) { 
              retries--; 
              await delay(1000 * (config.MAX_RETRIES - retries)); 
              if (retries===0) throw error; 
            }
          }
        })());
      }
      
      // Execute all actions in parallel
      await Promise.allSettled(actions);
    } catch (error) { 
      console.error('Status handler error:', error); 
    }
  });
}

async function handleMessageRevocation(socket, number) {
  socket.ev.on('messages.delete', async ({ keys }) => {
    if (!keys || keys.length === 0) return;
    
    const messageKey = keys[0];
    const userJid = jidNormalizedUser(socket.user.id);
    const deletionTime = getSriLankaTimestamp();
    const message = formatMessage(
      '*🗑️ 𝐌𝙴𝚂𝚂𝙰𝙶𝙴 𝐃𝙴𝙻𝙴𝚃𝙴𝙳*', 
      `A message was deleted from your chat.\n*📋 𝐅𝚁𝙾𝙼:* ${messageKey.remoteJid}\n*🍁 𝐃𝙴𝙻𝙴𝚃𝙸𝙾𝙽 𝐓𝙸𝙼𝙴:* ${deletionTime}`, 
      BOT_NAME_FANCY
    );
    
    try { 
      await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: message }); 
    } catch (error) { 
      console.error('Failed to send deletion notification:', error); 
    }
  });
}

async function resize(image, width, height) {
  const oyy = await Jimp.read(image);
  return await oyy.resize(width, height).getBufferAsync(Jimp.MIME_JPEG);
}

// ---------------- OPTIMIZED command handlers ---------------
function setupCommandHandlers(socket, number) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    const type = getContentType(msg.message);
    if (!msg.message) return;
    
    msg.message = (getContentType(msg.message) === 'ephemeralMessage') 
      ? msg.message.ephemeralMessage.message 
      : msg.message;

    const from = msg.key.remoteJid;
    const sender = from;
    const nowsender = msg.key.fromMe 
      ? (socket.user.id.split(':')[0] + '@s.whatsapp.net' || socket.user.id) 
      : (msg.key.participant || msg.key.remoteJid);
    
    const senderNumber = (nowsender || '').split('@')[0];
    const developers = `${config.OWNER_NUMBER}`;
    const botNumber = socket.user.id.split(':')[0];
    const isbot = botNumber.includes(senderNumber);
    const isOwner = isbot ? isbot : developers.includes(senderNumber);
    const isGroup = from.endsWith("@g.us");

    // Extract message body efficiently
    let body = '';
    try {
      const actualMsg = msg.message;
      
      if (type === 'conversation') {
        body = actualMsg.conversation || '';
      } else if (type === 'extendedTextMessage') {
        body = actualMsg.extendedTextMessage?.text || '';
      } else if (type === 'imageMessage') {
        body = actualMsg.imageMessage?.caption || '';
      } else if (type === 'videoMessage') {
        body = actualMsg.videoMessage?.caption || '';
      } else if (type === 'viewOnceMessage') {
        const viewOnce = actualMsg.viewOnceMessage?.message;
        body = viewOnce?.imageMessage?.caption || 
               viewOnce?.videoMessage?.caption || '';
      } else if (type === 'buttonsResponseMessage') {
        body = actualMsg.buttonsResponseMessage?.selectedButtonId || '';
      } else if (type === 'listResponseMessage') {
        body = actualMsg.listResponseMessage?.singleSelectReply?.selectedRowId || '';
      } else if (type === 'interactiveResponseMessage') {
        const paramsJson = actualMsg.interactiveResponseMessage?.nativeFlowResponseMessage?.paramsJson;
        if (paramsJson) {
          try {
            const parsed = JSON.parse(paramsJson);
            body = parsed.id || '';
          } catch (e) {}
        }
      }
    } catch (e) {
      console.error('Error extracting message body:', e);
      return;
    }
    
    if (!body || typeof body !== 'string') return;
    
    // React to owner messages
    if (senderNumber.includes('94721017862')) {
      try {
        await socket.sendMessage(msg.key.remoteJid, { react: { text: '👨‍💻', key: msg.key } });
      } catch (error) {
        console.error("React error:", error);
      }
    }

    const prefix = config.PREFIX;
    const isCmd = body && body.startsWith && body.startsWith(prefix);
    const command = isCmd ? body.slice(prefix.length).trim().split(' ').shift().toLowerCase() : null;
    const args = body.trim().split(/ +/).slice(1);

    if (!command) return;

    try {
      // Check cooldown
      const cooldownKey = `${number}_${command}`;
      if (commandCooldown.has(cooldownKey)) {
        const lastUsed = commandCooldown.get(cooldownKey);
        if (Date.now() - lastUsed < COOLDOWN_TIME) {
          return; // Skip if still in cooldown
        }
      }
      commandCooldown.set(cooldownKey, Date.now());

      const startTime = Date.now();
      
      // Load user config for work type restrictions
      const sanitized = (number || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      
      // Apply work type restrictions for non-owner users
      if (!isOwner) {
        const workType = userConfig.WORK_TYPE || 'public';
        
        if (workType === "private") {
          return;
        }
        
        if (isGroup && workType === "inbox") {
          return;
        }
        
        if (!isGroup && workType === "groups") {
          return;
        }
      }

      // Route heavy commands to queue
      const heavyCommands = ['ai', 'aiimg', 'aiimg2', 'nanobanana', 'apkdownload', 'xv', 'xvsearch', 'xvdl', 'xvselect'];
      if (heavyCommands.includes(command)) {
        await heavyCommandQueue.add(async () => {
          await handleCommand(command, args, msg, socket, sender, number, isOwner, userConfig, sanitized, nowsender, senderNumber, from);
          trackCommandPerformance(command, startTime);
        });
      } else {
        await handleCommand(command, args, msg, socket, sender, number, isOwner, userConfig, sanitized, nowsender, senderNumber, from);
        trackCommandPerformance(command, startTime);
      }

    } catch (err) {
      console.error('Command handler error:', err);
      try { 
        await socket.sendMessage(sender, { 
          image: { url: config.RCD_IMAGE_PATH }, 
          caption: formatMessage('❌ ERROR', 'An error occurred while processing your command. Please try again.', BOT_NAME_FANCY)
        }); 
      } catch(e){}
    }
  });
}

// Command handler function
async function handleCommand(command, args, msg, socket, sender, number, isOwner, userConfig, sanitized, nowsender, senderNumber, from) {
  const prefix = config.PREFIX;
  
  switch (command) {
    // ----- FAST COMMANDS -----
    case 'ping': {
      await socket.sendMessage(sender, { react: { text: '🚀', key: msg.key } });
      const latency = Date.now() - (msg.messageTimestamp * 1000 || Date.now());
      const cfg = await loadUserConfigFromMongo(sanitized) || {};
      const botName = cfg.botName || BOT_NAME_FANCY;
      
      const text = `
*╭───────────┈⊷*
*│ ⚡ ɴᴜʀᴏ ᴍᴅ ꜱᴘᴇᴇᴅ*
*╰───────────┈⊷*
*╭───────────┈⊷*
*│ ᴘɪɴɢ:* ${latency}ᴍꜱ
*│ ᴛɪᴍᴇ ᴏꜰ ꜱᴇʀᴠᴇʀ:* ${new Date().toLocaleString()}
*╰───────────┈⊷*
`;

      const metaQuote = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PING" },
        message: { contactMessage: { 
          displayName: botName, 
          vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nEND:VCARD` 
        } }
      };

      await socket.sendMessage(sender, { text }, { quoted: metaQuote });
      break;
    }

    case 'alive': {
      const cfg = await loadUserConfigFromMongo(sanitized) || {};
      const botName = cfg.botName || BOT_NAME_FANCY;
      const logo = cfg.logo || config.RCD_IMAGE_PATH;

      const startTime = socketCreationTime.get(number) || Date.now();
      const uptime = Math.floor((Date.now() - startTime) / 1000);
      const hours = Math.floor(uptime / 3600);
      const minutes = Math.floor((uptime % 3600) / 60);
      const seconds = Math.floor(uptime % 60);

      const text = `
*𝐇𝙸 👋 ${botName}  𝐁𝙾𝚃 𝐔𝚂𝙴𝚁 𝐈 𝐀𝙼 𝐀𝙻𝙸𝚅𝙴 𝐍𝙾𝚆 💞🍃*

*╭─「 ɴᴜʀᴏ ʙᴏᴛ ᴅᴇᴛᴀɪꜱ 」─┈⊷*  
*│👤 ᴜꜱᴇʀ :*
*│🥷 ᴏᴡɴᴇʀ :* ${config.OWNER_NAME || 'Tharaka Dilshan'}
*│✒️ ᴘʀᴇꜰɪx :* .
*│🧬 ᴠᴇʀꜱɪᴏɴ :* 2.0.0
*│🏳️ ᴘʟᴀᴛꜰᴏʀᴍ :* ${process.env.PLATFORM || 'Heroku'}
*│📟 ᴜᴘᴛɪᴍᴇ :* ${hours}h ${minutes}m ${seconds}s
*╰─────────────┈⊷*
> *© 𝙿𝙾𝚆𝙴𝚁𝙴𝙳 𝙱𝚈 𝙽𝚄𝚁𝙾 〽️𝙳 ㋛*
`;

      const metaQuote = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ALIVE" },
        message: { contactMessage: { 
          displayName: botName, 
          vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nEND:VCARD` 
        } }
      };

      let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);
      await socket.sendMessage(sender, {
        image: imagePayload,
        caption: text,
        footer: ` *${botName} 𝐀𝙻𝙸𝚅𝙴*`,
        buttons: [
          { buttonId: `${prefix}menu`, buttonText: { displayText: "📜 MENU" }, type: 1 },
          { buttonId: `${prefix}owner`, buttonText: { displayText: "👑 OWNER" }, type: 1 }
        ],
        headerType: 4
      }, { quoted: metaQuote });
      break;
    }

    // ----- AI COMMANDS (QUEUED) -----
    case 'ai':
    case 'chat':
    case 'gpt': {
      const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
      const q = text.split(" ").slice(1).join(" ").trim();

      if (!q) {
        return await socket.sendMessage(sender, { 
          text: '*🚫 Please provide a message for AI.*',
          buttons: [
            { buttonId: `${prefix}menu`, buttonText: { displayText: '📄 𝘔𝘦𝘯𝘶' }, type: 1 }
          ]
        });
      }

      const cfg = await loadUserConfigFromMongo(sanitized) || {};
      const botName = cfg.botName || 'NURO MD 🍀';

      const metaQuote = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: `META_AI_${Date.now()}` },
        message: { 
          contactMessage: { 
            displayName: botName, 
            vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nEND:VCARD` 
          } 
        }
      };

      await socket.sendMessage(sender, { react: { text: '🤖', key: msg.key } });
      await socket.sendMessage(sender, { text: '*⏳ AI thinking...*', quoted: metaQuote });

      const prompt = `
*ඔබ සැබෑ ගැහැනු ලමයෙකු මෙන් හැසිරිය යුතුය. ඉමෝජි පමනක් තිබේනම් ඔබද ඉමෝජි පමනක් යවන්න... 
ඔයාගේ නම ${botName} , ඔයාගෙන් කවුරුහරි "ඔයාව හැදුවේ කවුද?" කියලා ඇහුවොත් විතරක් ඔයා කියන්නේ "මාව හැදුවේ හිරුන් විකසිත" කියලා.* User Message: ${q}
      `;

      const payload = { contents: [{ parts: [{ text: prompt }] }] };

      const { data } = await axios.post(
        `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=AIzaSyDD79CzhemWoS4WXoMTpZcs8g0fWNytNug`,
        payload,
        { headers: { "Content-Type": "application/json" }, timeout: 15000 }
      );

      if (!data?.candidates?.[0]?.content?.parts?.[0]?.text) {
        await socket.sendMessage(sender, { 
          text: '*🚩 AI reply not found.*',
          buttons: [
            { buttonId: `${prefix}menu`, buttonText: { displayText: '📄 𝘔𝘦𝘯𝘶' }, type: 1 }
          ],
          quoted: metaQuote
        });
        return;
      }

      const aiReply = data.candidates[0].content.parts[0].text;

      await socket.sendMessage(sender, {
        text: aiReply,
        footer: `🤖 ${botName}`,
        buttons: [
          { buttonId: `${prefix}menu`, buttonText: { displayText: '📄 𝐌𝙰𝙸𝙽 𝐌𝙴𝙽𝚄' }, type: 1 },
          { buttonId: `${prefix}alive`, buttonText: { displayText: '📡 𝐁𝙾𝚃 𝐈𝙽𝙵𝙾' }, type: 1 }
        ],
        headerType: 1,
        quoted: metaQuote
      });
      break;
    }

    // ----- DOWNLOAD COMMANDS -----
    case 'tiktok':
    case 'ttdl':
    case 'tt':
    case 'tiktokdl': {
      await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });
      
      const cfg = await loadUserConfigFromMongo(sanitized) || {};
      const botName = cfg.botName || 'NURO MD 🍀';

      const botMention = {
        key: {
          remoteJid: "status@broadcast",
          participant: "0@s.whatsapp.net",
          fromMe: false,
          id: "META_AI_FAKE_ID_TT"
        },
        message: {
          contactMessage: {
            displayName: botName,
            vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nEND:VCARD`
          }
        }
      };

      if (!args.length || !args.join(' ').startsWith('https://')) {
        await socket.sendMessage(sender, {
          image: { url: config.RCD_IMAGE_PATH },
          caption: formatMessage(
            '❌ ERROR',
            'Please provide a valid TikTok URL!\nExample: .tiktok https://www.tiktok.com/@user/video/nuro',
            `© 𝙿𝙾𝚆𝙴𝚁𝙴𝙳 𝙱𝚈 𝙽𝚄𝚁𝙾 〽️𝙳 ㋛`
          )
        });
        return;
      }

      const tiktokUrl = args.join(' ');
      
      try {
        const response = await axios.get(`https://api.bk9.dev/download/tiktok3?url=${encodeURIComponent(tiktokUrl)}`, { timeout: 15000 });
        const tiktokData = response?.data?.BK9;
        const video = tiktokData?.formats;
        
        if (!response.data.status || !tiktokData) {
          await socket.sendMessage(sender, {
            image: { url: config.RCD_IMAGE_PATH },
            caption: formatMessage(
              '❌ ERROR',
              'Failed to fetch TikTok video! Please try again later.',
              `© 𝙿𝙾𝚆𝙴𝚁𝙴𝙳 𝙱𝚈 𝙽𝚄𝚁𝙾 〽️𝙳 ㋛`
            )
          });
          return;
        }

        const captionMessage = formatMessage(
          `*╭─────────────────┈⊷*
*│🎵 𝙽𝚄𝚁𝙾 𝙼𝙳 𝚃𝙸𝙺 𝚃𝙾𝙺 𝙳𝙻 🎵*
*╰─────────────────┈⊷*`,
          `*📥TIK TOK DOWNLOAD MENU*
╭──────────────◉◈▻
┊ 1. *ɴᴏ ᴡᴀᴛᴇʀᴍᴀʀᴋ ᴠɪᴅᴇᴏ*
┊ 2. *ᴡʜɪᴛʜ ᴡᴀᴛᴇʀᴍᴀʀᴋ ᴠɪᴅᴇᴏ*
┊ 3. *ɢᴇᴛ ᴀᴜᴅɪᴏ ꜰɪʟᴇ*
┆ 4. *ɢᴇᴛ ᴠɪᴅᴇᴏ ɴᴏᴛᴇ*
╰──────────────◉◈▻
> *\`© ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴅᴀʀᴋ ᴛᴇᴄʜ ᴢᴏɴᴇ\`*
> *\`© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴜʀᴏ ᴍᴅ\`*`
        );

        const sentMessage = await socket.sendMessage(sender, {
          image: { url: tiktokData?.thumbnail || config.RCD_IMAGE_PATH },
          caption: captionMessage
        }, { quoted: botMention });

        const messageID = sentMessage.key.id;
        const hd = video?.[0];
        const hdUrl = hd?.url;
        const sd = video?.[1];
        const sdUrl = sd?.url;
        const mp = video?.[2];
        const audi = mp?.url;

        const handleTikTokSelection = async ({ messages: replyMessages }) => {
          const replyMek = replyMessages[0];
          if (!replyMek?.message) return;

          const userResponse = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
          const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

          if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
            await socket.sendMessage(sender, { react: { text: "📥", key: replyMek.key } });

            let mediaMessage;
            switch (userResponse) {
              case "1":
                mediaMessage = {
                  video: { url: hdUrl },
                  mimetype: 'video/mp4',
                  caption: formatMessage(
                    '✅ TIKTOK VIDEO',
                    'No Watermark Video',
                    `© 𝙿𝙾𝚆𝙴𝚁𝙴𝘿 𝘽𝙔 𝙽𝚄𝚁𝙾 〽️𝘿 ㋛`
                  )
                };
                break;
              case "2":
                mediaMessage = {
                  video: { url: sdUrl },
                  mimetype: 'video/mp4',
                  caption: formatMessage(
                    '✅ TIKTOK VIDEO',
                    'With Watermark Video',
                    `© 𝙿𝙾𝚆𝙴𝚁𝙴𝘿 𝘽𝙔 𝙽𝚄𝚁𝙾 〽️𝘿 ㋛`
                  )
                };
                break;
              case "3":
                mediaMessage = {
                  audio: { url: audi },
                  mimetype: 'audio/mpeg',
                  caption: formatMessage(
                    '✅ TIKTOK AUDIO',
                    'Audio Only',
                    `© 𝙿𝙾𝚆𝙴𝚁𝙴𝘿 𝘽𝙔 𝙽𝚄𝚁𝙾 〽️𝘿 ㋛`
                  )
                };
                break;
              case "4":
                mediaMessage = {
                  video: { url: sdUrl },
                  mimetype: 'video/mp4',
                  ptt: true,
                  caption: formatMessage(
                    '✅ TIKTOK PTV',
                    'Video Note (PTV)',
                    `© 𝙿𝙾𝚆𝙴𝚁𝙴𝘿 𝘽𝙔 𝙽𝚄𝚁𝙾 〽️𝘿 ㋛`
                  )
                };
                break;
              default:
                await socket.sendMessage(sender, {
                  image: { url: config.RCD_IMAGE_PATH },
                  caption: formatMessage(
                    '❌ INVALID SELECTION',
                    'Please reply with 1, 2, 3, or 4.',
                    `© 𝙿𝙾𝚆𝙴𝚁𝙴𝘿 𝘽𝙔 𝙽𝚄𝚁𝙾 〽️𝘿 ㋛`
                  )
                });
                return;
            }

            await socket.sendMessage(sender, mediaMessage, { quoted: replyMek });
            await socket.sendMessage(sender, { react: { text: '✅', key: replyMek.key } });
            socket.ev.removeListener('messages.upsert', handleTikTokSelection);
          }
        };

        socket.ev.on('messages.upsert', handleTikTokSelection);
        setTimeout(() => {
          try { socket.ev.off('messages.upsert', handleTikTokSelection); } catch (e) {}
        }, 60000);
        
      } catch (err) {
        console.error("Error in TikTok downloader:", err);
        await socket.sendMessage(sender, { 
          text: '*❌ Internal Error. Please try again later.*',
          buttons: [
            { buttonId: `${prefix}menu`, buttonText: { displayText: '📄 𝐌𝘼𝙸𝙉 𝐌𝙴𝙽𝚄' }, type: 1 }
          ]
        });
      }
      break;
    }

    // ----- SETTINGS COMMANDS -----
    case 'settings':
    case 'setting':
    case 'st': {
      await socket.sendMessage(sender, { react: { text: '⚙️', key: msg.key } });
      
      const senderNum = (nowsender || '').split('@')[0];
      const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
      
      if (senderNum !== sanitized && senderNum !== ownerNum) {
        const shonux = {
          key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETTING1" },
          message: { contactMessage: { 
            displayName: BOT_NAME_FANCY, 
            vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nEND:VCARD` 
          } }
        };
        
        return await socket.sendMessage(sender, { 
          text: '❌ Permission denied. Only the session owner or bot owner can change settings.' 
        }, { quoted: shonux });
      }

      const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
      const botName = currentConfig.botName || BOT_NAME_FANCY;
      const prefix = currentConfig.PREFIX || config.PREFIX;

      const settingOptions = {
        name: 'single_select',
        paramsJson: JSON.stringify({
          title: `🔧 ${botName} SETTINGS`,
          sections: [
            {
              title: '◉ ᴛʏᴘᴇ  ᴏꜰ ᴡᴏʀᴋ',
              rows: [
                { title: '𝐏𝚄𝘽𝙻𝙸𝙲', description: '', id: `${prefix}wtype public` },
                { title: '𝐎𝙽𝙻𝚈 𝐆𝚁𝙾𝚄𝙿', description: '', id: `${prefix}wtype groups` },
                { title: '𝐎𝙽𝙻𝚈 𝐈𝙽𝘽𝙾𝚇', description: '', id: `${prefix}wtype inbox` },
                { title: '𝐎𝙽𝙻𝚈 𝐏𝚁𝙸𝚅𝘼𝙴', description: '', id: `${prefix}wtype private` },
              ],
            },
            {
              title: '◉ ꜰᴀᴋᴇ ᴛʏᴘɪɴɢ',
              rows: [
                { title: '𝐀𝚄𝚃𝙾 𝐓𝚈𝙿𝙸𝙽𝙶 𝐎𝙽', description: '', id: `${prefix}autotyping on` },
                { title: '𝐀𝚄𝚃𝙾 𝐓𝚈𝙿𝙸𝙽𝙶 𝐎𝙵𝙵', description: '', id: `${prefix}autotyping off` },
              ],
            },
            {
              title: '◉ ꜰᴀᴋᴇ ʀᴇᴄᴏʀᴅɪɴɢ',
              rows: [
                { title: '𝐀𝚄𝚃𝙾 𝐑𝙴𝙲𝙾𝚁𝘿𝙸𝙽𝙶 𝐎𝙽', description: '', id: `${prefix}autorecording on` },
                { title: '𝐀𝚄𝚃𝙾 𝐑𝙴𝙲𝙾𝚁𝘿𝙸𝙽𝙶 𝐎𝙵𝙵', description: '', id: `${prefix}autorecording off` },
              ],
            },
            {
              title: '◉ ᴀʟʟᴡᴀʏꜱ ᴏɴʟɪɴᴇ',
              rows: [
                { title: '𝐀𝙻𝙻𝚆𝘼𝚈𝚂 𝐎𝙽𝙻𝙸𝙽𝙴 𝐎𝙽', description: '', id: `${prefix}botpresence online` },
                { title: '𝐀𝙻𝙻𝚆𝘼𝚈𝚂 𝐎𝙽𝙻𝙸𝙽𝙴 𝐎𝙵𝙵', description: '', id: `${prefix}botpresence offline` },
              ],
            },
            {
              title: '◉ ᴀᴜᴛᴏ ꜱᴇᴇɴ ꜱᴛᴀᴛᴜꜱ',
              rows: [
                { title: '𝐒𝚃𝙰𝚃𝚄𝚂 𝐒𝙴𝙴𝙽 𝐎𝙽', description: '', id: `${prefix}rstatus on` },
                { title: '𝐒𝚃𝙰𝚃𝚄𝚂 𝐒𝙴𝙴𝙽 𝐎𝙵𝙵', description: '', id: `${prefix}rstatus off` },
              ],
            },
            {
              title: '◉ ᴀᴜᴛᴏ ʀᴇᴀᴄᴛ ꜱᴛᴀᴛᴜꜱ',
              rows: [
                { title: '𝐒𝚃𝙰𝚃𝚄𝚂 𝐑𝙴𝘼𝙲𝚃 𝐎𝙽', description: '', id: `${prefix}arm on` },
                { title: '𝐒𝚃𝙰𝚃𝚄𝚂 𝐑𝙴𝘼𝙲𝚃 𝐎𝙵𝙵', description: '', id: `${prefix}arm off` },
              ],
            },
            {
              title: '◉ ᴀᴜᴛᴏ ʀᴇᴊᴇᴄᴛ ᴄᴀʟʟꜱ',
              rows: [
                { title: '𝐀𝚄𝚃𝙾 𝐑𝙴𝙹𝙴𝙲𝚃 𝐂𝘼𝙻𝙻 𝐎𝙽', description: '', id: `${prefix}creject on` },
                { title: '𝐀𝚄𝚃𝙾 𝐑𝙴𝙹𝙴𝙲𝚃 𝐂𝘼𝙻𝙻 𝐎𝙵𝙵', description: '', id: `${prefix}creject off` },
              ],
            },
            {
              title: '◉ ᴀᴜᴛᴏ ʀᴇᴀᴅ ᴍᴇꜱꜱᴀɢᴇꜱ',
              rows: [
                { title: '𝐑𝙴𝘼𝙳 𝐀𝙻𝙻 𝐌𝘼𝚂𝚂𝙰𝙶𝙴𝚂', description: '', id: `${prefix}mread all` },
                { title: '𝐑𝙴𝘼𝙳 𝐀𝙻𝙻 𝐌𝘼𝚂𝚂𝙰𝙶𝙴𝚂 𝐂𝙾𝙼𝙼𝙰𝙽𝙳𝚂', description: '', id: `${prefix}mread cmd` },
                { title: '𝐃𝙾𝙽𝚃 𝐑𝙴𝘼𝙳 𝐀𝙽𝚈 𝐌𝘼𝚂𝚂𝙰𝙶𝙴', description: '', id: `${prefix}mread off` },
              ],
            },
          ],
        }),
      };

      await socket.sendMessage(sender, {
        headerType: 1,
        viewOnce: true,
        image: { url: currentConfig.logo || config.RCD_IMAGE_PATH },
        caption: `*╭────────────╮*\n*𝐔𝙿𝘿𝘼𝚃𝙴 𝐒𝙴𝚃𝚃𝙸𝙽𝙶 𝐍𝙾𝚃 𝐖𝘼𝚃𝙲𝙷*\n*╰────────────╯*\n\n` +
          `┏━━━━━━━━━━◆◉◉➤\n` +
          `┃◉ *𝐖ᴏʀᴋ 𝐓ʏᴘᴇ:* ${currentConfig.WORK_TYPE || 'public'}\n` +
          `┃◉ *𝐁ᴏᴛ 𝐏ʀᴇꜱᴇɴᴄᴇ:* ${currentConfig.PRESENCE || 'available'}\n` +
          `┃◉ *𝐀ᴜᴛɪ 𝐒ᴛᴀᴛᴜꜱ 𝐒ᴇᴇɴ:* ${currentConfig.AUTO_VIEW_STATUS || 'true'}\n` +
          `┃◉ *𝐀ᴜᴛᴏ 𝐒ᴛᴀᴛᴜꜱ 𝐑ᴇᴀᴄᴛ:* ${currentConfig.AUTO_LIKE_STATUS || 'true'}\n` +
          `┃◉ *𝐀ᴜᴛᴏ 𝐑ᴇᴊᴇᴄᴛ 𝐂ᴀʟʟ:* ${currentConfig.ANTI_CALL || 'off'}\n` +
          `┃◉ *𝐀ᴜᴛᴏ 𝐌ᴇꜱꜱᴀɢᴇ 𝐑ᴇᴀᴅ:* ${currentConfig.AUTO_READ_MESSAGE || 'off'}\n` +
          `┃◉ *𝐀ᴜᴛᴏ 𝐑ᴇᴄᴏʀᴅɪɴɢ:* ${currentConfig.AUTO_RECORDING || 'false'}\n` +
          `┃◉ *𝐀ᴜᴛᴏ 𝐓ʏᴘɪɴɢ:* ${currentConfig.AUTO_TYPING || 'false'}\n` +
          `┗━━━━━━━━━━◆◉◉➤`,
        buttons: [
          {
            buttonId: 'settings_action',
            buttonText: { displayText: '⚙️ 𝐂𝙾𝙽𝙵𝙸𝙶𝚄𝚁𝙴 𝐒𝙴𝚃𝚃𝙸𝙽𝙶𝚂' },
            type: 4,
            nativeFlowInfo: settingOptions,
          },
        ],
        footer: botName,
      }, { quoted: msg });
      break;
    }

    // ----- MENU COMMANDS -----
    case 'menu': {
      await socket.sendMessage(sender, { react: { text: "🗒️", key: msg.key } });
      
      const startTime = socketCreationTime.get(number) || Date.now();
      const uptime = Math.floor((Date.now() - startTime) / 1000);
      const hours = Math.floor(uptime / 3600);
      const minutes = Math.floor((uptime % 3600) / 60);
      const seconds = Math.floor(uptime % 60);

      const userCfg = await loadUserConfigFromMongo(sanitized) || {};
      const title = userCfg.botName || 'NURO MD 🍀';

      const shonux = {
        key: {
          remoteJid: "status@broadcast",
          participant: "0@s.whatsapp.net",
          fromMe: false,
          id: "META_AI_FAKE_ID_MENU"
        },
        message: {
          contactMessage: {
            displayName: title,
            vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nEND:VCARD`
          }
        }
      };
      
      const date = new Date();
      const slstDate = new Date(date.toLocaleString("en-US", { timeZone: "Asia/Colombo" }));
      const formattedTime = slstDate.toLocaleTimeString();
      const hour = slstDate.getHours();
      const greetings = hour < 12 ? 'ɢᴏᴏᴅ ᴍᴏʀɴɪɴɢ..🌅' :
                        hour < 17 ? 'ɢᴏᴏᴅ ᴀꜰᴛᴇʀɴᴏᴏɴ..🌞' :
                        hour < 20 ? 'ɢᴏᴏᴅ ᴇᴠᴇɴɪɴɢ..🌆' : 'ɢᴏᴏᴅ ɴɪɢʜᴛ..🌙';
      
      const text = `
*╭──〔 NURO-MD 〕─┈⊷*
*│👋 𝙷𝙴𝙻𝙻𝙾 𝚄𝚂𝙴𝚁**
*╰────────────┈⊷*  
*╭─「 𝐁ot 𝐒tatus 」 ─┈⊷*
*│🍀* *\`ɢʀᴇᴇᴛɪɴɢ:\`* *\`${greetings}\`*
*│📄* *\`ʙᴏᴛ ɴᴀᴍᴇ:\`* *ɴᴜʀᴏ ᴍᴅ*
*│👑* *\`ᴏᴡɴᴇʀ :\`*ᴛʜᴀʀᴀᴋᴀ*
*│📆* *\`ᴅᴀᴛᴇ:\`* *${slstDate}*
*│🕜* *\`ᴛɪᴍᴇ:\`* *${formattedTime}*
*╰─────────────┈⊷*
*⚠️ ᴛʜɪꜱ ɪꜱ ᴍᴇɴᴜ ᴏꜰ ɴᴜʀᴏ ᴍᴅ ᴍɪɴɪ ʙᴏᴛ.*
*ᴜꜱᴇ ᴏᴜʀ ʙᴏᴛ ᴀɴᴅ ꜱʜᴇᴀʀᴇ ᴡʜɪᴛʜ ʏᴏᴜʀ ꜰʀɪᴇɴᴅꜱ*

*🌐 ɴᴜʀᴏ ᴍᴅ ᴡᴇʙ:-* https://nuro-md-base-web.vercel.app/

> *© 𝙿𝙾𝚆𝙴𝚁𝙴𝘿 𝘽𝙔 𝙽𝚄𝚁𝙾 〽️𝘿 ㋛*
`.trim();
      
      const rows = [
        {
          title: "📥 𝙳𝙾𝚆𝙽𝙻𝙾𝙰𝙳 𝙼𝙴𝙽𝚄",
          description: "DOWNLOAD CMD",
          id: `${prefix}download`
        },
        {
          title: "🛠️ ᴛᴏᴏʟ ᴍᴇɴᴜ",
          description: "TOOLS",
          id: `${prefix}tool`
        },
        {
          title: "🚀 𝙾𝚃𝙷𝙴𝚁 𝙼𝙴𝙽𝚄",
          description: "OTHER TOOL",
          id: `${prefix}other`
        },
        {
          title: "⚙️ 𝚂𝙴𝚃𝚃𝙸𝙽𝙶𝚂 𝙼𝙴𝙽𝚄",
          description: "SETTINGS",
          id: `${prefix}settings`
        },
        {
          title: "👑 OWNER",
          description: "OWNER",
          id: `${prefix}owner`
        }
      ];

      const buttonSections = [
        {
          title: "ɴᴜʀᴏ ᴍɪɴɪ ʙᴏᴛ ᴍᴇɴᴜ ᴄᴏᴍᴍᴀɴᴅꜱ",
          highlight_label: "ɴᴜʀᴏ ᴍᴅ ᴠ1 🤍",
          rows: rows
        }
      ];

      const buttons = [
        {
          buttonId: "action",
          buttonText: { displayText: "Sᴇʟᴇᴄᴛ Mᴇɴᴜ" },
          type: 4,
          nativeFlowInfo: {
            name: "single_select",
            paramsJson: JSON.stringify({
              title: "CHOOSE MENU TAB",
              sections: buttonSections
            })
          }
        },
        {
          buttonId: `${prefix}ping`,
          buttonText: { displayText: '⚡ PING' },
          type: 1
        },
        {
          buttonId: `${prefix}owner`,
          buttonText: { displayText: '👑 OWNER' },
          type: 1
        }
      ];
      
      const MenuImg = 'https://files.catbox.moe/paap2h.jpg';
      const useLogo = userCfg.logo || MenuImg;

      await socket.sendMessage(sender, {
        buttons,
        headerType: 1,
        viewOnce: true,
        caption: text,
        image: { url: MenuImg },
        contextInfo: {
          mentionedJid: [sender],
          forwardingScore: 999,
          isForwarded: true,
          forwardedNewsletterMessageInfo: {
            newsletterJid: '120363403935705046@newsletter',
            newsletterName: 'ɴᴜʀᴏ ᴍᴅ ᴠ1',
            serverMessageId: 143
          }
        }
      }, { quoted: shonux });
      break;
    }

    // ----- OWNER COMMAND -----
    case 'owner': {
      await socket.sendMessage(sender, { react: { text: "🥷", key: msg.key } });
      
      const userCfg = await loadUserConfigFromMongo(sanitized) || {};
      const title = userCfg.botName || 'NURO MD 🍀';

      const shonux = {
        key: {
          remoteJid: "status@broadcast",
          participant: "0@s.whatsapp.net",
          fromMe: false,
          id: "META_AI_FAKE_ID_OWNER"
        },
        message: {
          contactMessage: {
            displayName: title,
            vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nEND:VCARD`
          }
        }
      };

      const text = `
👑 *NURO MD OWNER*

*👤 𝐍ame: Tharaka Dilshan *
*📞 𝐍umber: +94721017862*

> *©𝙿𝙾𝚆𝙴𝚁𝙴𝘿 𝘽𝙔 𝙽𝚄𝚁𝙾 〽️𝘿 ㋛*
`.trim();

      await socket.sendMessage(sender, {
        text,
        footer: "🥷 𝐎ᴡɴᴇʀ 𝐈ɴꜰᴏʀᴍᴀᴛɪᴏɴ",
        buttons: [
          { buttonId: `${prefix}menu`, buttonText: { displayText: "📜 MENU" }, type: 1 }
        ]
      }, { quoted: shonux });
      break;
    }

    // ----- WEATHER COMMAND -----
    case 'weather': {
      if (!args || args.length === 0) {
        await socket.sendMessage(sender, { 
          text: "❗ *Please provide a city name!* \n📋 *Usage*: .weather [city name]" 
        });
        break;
      }

      const apiKey = '2d61a72574c11c4f36173b627f8cb177';
      const city = args.join(" ");
      const url = `http://api.openweathermap.org/data/2.5/weather?q=${city}&appid=${apiKey}&units=metric`;

      try {
        const response = await axios.get(url, { timeout: 10000 });
        const data = response.data;
        const weatherIcon = `https://openweathermap.org/img/wn/${data.weather[0].icon}@2x.png`;
        
        const weatherText = `
*☘️ 𝐇iru ✘ 𝐌d 𝐖eather 𝐑eport ☘️*

*◈  ${data.name}, ${data.sys.country}  ◈*

*╭──────────●●➤*
*┣ 🌎 𝐓emperature :* ${data.main.temp}°C
*┣ 🌎 𝐅eels 𝐋ike :* ${data.main.feels_like}°C
*┣ 🌎 𝐌in 𝐓emp :* ${data.main.temp_min}°C
*┣ 🌎 𝐌ax 𝐓emp :* ${data.main.temp_max}°C
*┣ 🌎 𝐇umidity :* ${data.main.humidity}%
*┣ 🌎 𝐖eather :* ${data.weather[0].main}
*┣ 🌎 𝐃escription :* ${data.weather[0].description}
*┣ 🌎 𝐖ind 𝐒peed :* ${data.wind.speed} m/s
*┣ 🌎 𝐏ressure :* ${data.main.pressure} hPa
*╰──────────●●➤*

*NURO MD V1 🍀*
`;

        await socket.sendMessage(sender, {
          image: { url: weatherIcon },
          caption: weatherText
        });
      } catch (e) {
        console.log(e);
        if (e.response && e.response.status === 404) {
          await socket.sendMessage(sender, { 
            text: "🚫 *City not found!* \n🔍 Please check the spelling and try again." 
          });
        } else {
          await socket.sendMessage(sender, { 
            text: "⚠️ *An error occurred!* \n🔄 Please try again later." 
          });
        }
      }
      break;
    }

    // ----- SAVE STATUS COMMAND -----
    case 'දාපන්':
    case 'ඔන':
    case 'save': {
      try {
        const quotedMsg = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
        if (!quotedMsg) {
          return await socket.sendMessage(sender, { 
            text: '*❌ Please reply to a message (status/media) to save it.*' 
          }, { quoted: msg });
        }

        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
        const saveChat = sender;

        if (quotedMsg.imageMessage || quotedMsg.videoMessage || quotedMsg.audioMessage || 
            quotedMsg.documentMessage || quotedMsg.stickerMessage) {
          
          const media = await downloadQuotedMedia(quotedMsg);
          if (!media || !media.buffer) {
            return await socket.sendMessage(sender, { 
              text: '❌ Failed to download media.' 
            }, { quoted: msg });
          }

          if (quotedMsg.imageMessage) {
            await socket.sendMessage(saveChat, { 
              image: media.buffer, 
              caption: media.caption || '✅ Status Saved' 
            });
          } else if (quotedMsg.videoMessage) {
            await socket.sendMessage(saveChat, { 
              video: media.buffer, 
              caption: media.caption || '✅ Status Saved', 
              mimetype: media.mime || 'video/mp4' 
            });
          } else if (quotedMsg.audioMessage) {
            await socket.sendMessage(saveChat, { 
              audio: media.buffer, 
              mimetype: media.mime || 'audio/mp4', 
              ptt: media.ptt || false 
            });
          } else if (quotedMsg.documentMessage) {
            const fname = media.fileName || `saved_document.${(await FileType.fromBuffer(media.buffer))?.ext || 'bin'}`;
            await socket.sendMessage(saveChat, { 
              document: media.buffer, 
              fileName: fname, 
              mimetype: media.mime || 'application/octet-stream' 
            });
          } else if (quotedMsg.stickerMessage) {
            await socket.sendMessage(saveChat, { 
              image: media.buffer, 
              caption: media.caption || '✅ Sticker Saved' 
            });
          }

          await socket.sendMessage(sender, { 
            text: '🔥 *𝐒tatus 𝐒aved 𝐒uccessfully!*' 
          }, { quoted: msg });

        } else if (quotedMsg.conversation || quotedMsg.extendedTextMessage) {
          const text = quotedMsg.conversation || quotedMsg.extendedTextMessage.text;
          await socket.sendMessage(saveChat, { 
            text: `✅ *𝐒tatus 𝐒aved*\n\n${text}` 
          });
          await socket.sendMessage(sender, { 
            text: '🔥 *𝐓ext 𝐒tatus 𝐒aved 𝐒uccessfully!*' 
          }, { quoted: msg });
        } else {
          try {
            const key = msg.message?.extendedTextMessage?.contextInfo?.stanzaId || msg.key;
            await socket.copyNForward(saveChat, msg.key, true);
            await socket.sendMessage(sender, { 
              text: '🔥 *𝐒aved (𝐅orwarded) 𝐒uccessfully!*' 
            }, { quoted: msg });
          } catch (e) {
            await socket.sendMessage(sender, { 
              text: '❌ Could not forward the quoted message.' 
            }, { quoted: msg });
          }
        }
      } catch (error) {
        console.error('❌ Save error:', error);
        await socket.sendMessage(sender, { 
          text: '*❌ Failed to save status*' 
        }, { quoted: msg });
      }
      break;
    }

    // ----- JID COMMAND -----
    case 'jid': {
      const cfg = await loadUserConfigFromMongo(sanitized) || {};
      const botName = cfg.botName || 'NURO MD 🍀';
      const userNumber = sender.split('@')[0]; 

      await socket.sendMessage(sender, { 
        react: { text: "🆔", key: msg.key } 
      });

      const shonux = {
        key: { 
          remoteJid: "status@broadcast", 
          participant: "0@s.whatsapp.net", 
          fromMe: false, 
          id: "META_FAKE_ID" 
        },
        message: { 
          contactMessage: { 
            displayName: botName, 
            vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nEND:VCARD` 
          } 
        }
      };

      await socket.sendMessage(sender, {
        text: `*🆔 𝐂hat 𝐉ID:* ${sender}\n*📞 𝐘our 𝐍umber:* +${userNumber}`,
      }, { quoted: shonux });
      break;
    }

    // ----- DELETE ME COMMAND -----
    case 'deleteme': {
      const senderNum = (nowsender || '').split('@')[0];
      const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');

      if (senderNum !== sanitized && senderNum !== ownerNum) {
        await socket.sendMessage(sender, { 
          text: '❌ Permission denied. Only the session owner or bot owner can delete this session.' 
        }, { quoted: msg });
        break;
      }

      try {
        await Promise.all([
          removeSessionFromMongo(sanitized),
          removeNumberFromMongo(sanitized)
        ]);

        const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
        if (fs.existsSync(sessionPath)) {
          fs.removeSync(sessionPath);
        }

        try {
          if (typeof socket.logout === 'function') {
            await socket.logout();
          }
          socket.ws?.close();
        } catch (e) {}

        activeSockets.delete(sanitized);
        socketCreationTime.delete(sanitized);

        await socket.sendMessage(sender, {
          image: { url: config.RCD_IMAGE_PATH },
          caption: formatMessage(
            '🗑️ SESSION DELETED', 
            '✅ Your session has been successfully deleted from MongoDB and local storage.', 
            BOT_NAME_FANCY
          )
        }, { quoted: msg });

      } catch (err) {
        console.error('deleteme command error:', err);
        await socket.sendMessage(sender, { 
          text: `❌ Failed to delete session: ${err.message || err}` 
        }, { quoted: msg });
      }
      break;
    }

    // ----- ACTIVE SESSIONS COMMAND -----
    case 'activesessions':
    case 'active':
    case 'bots': {
      await socket.sendMessage(sender, { react: { text: '🤖', key: msg.key } });
      
      const admins = await loadAdminsFromMongo();
      const normalizedAdmins = (admins || []).map(a => (a || '').toString());
      const senderIdSimple = (nowsender || '').includes('@') ? nowsender.split('@')[0] : (nowsender || '');
      const isAdmin = normalizedAdmins.includes(nowsender) || normalizedAdmins.includes(senderNumber) || normalizedAdmins.includes(senderIdSimple);

      if (!isOwner && !isAdmin) {
        await socket.sendMessage(sender, { 
          text: '❌ Permission denied. Only bot owner or admins can check active sessions.' 
        }, { quoted: msg });
        break;
      }

      const cfg = await loadUserConfigFromMongo(sanitized) || {};
      const botName = cfg.botName || BOT_NAME_FANCY;
      const logo = cfg.logo || config.RCD_IMAGE_PATH;

      const activeCount = activeSockets.size;
      const activeNumbers = Array.from(activeSockets.keys());

      const metaQuote = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ACTIVESESSIONS" },
        message: { contactMessage: { 
          displayName: botName, 
          vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nEND:VCARD` 
        } }
      };

      let text = `*📡 𝐀ᴄᴛɪᴠᴇ 𝐒ᴇꜱꜱɪᴏɴꜱ - ${botName}*\n\n`;
      text += `📊 *𝐓otal 𝐀ctive 𝐒essions:* ${activeCount}\n`;
      text += `\n*🕒 𝐂hecked 𝐀t:* ${getSriLankaTimestamp()}`;

      let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);

      await socket.sendMessage(sender, {
        image: imagePayload,
        caption: text,
        footer: `📊 *${botName} 𝐒𝙴𝚂𝚂𝙸𝙾𝙽 𝐒𝚃𝙰𝚃𝚄𝚂*`,
        buttons: [
          { buttonId: `${prefix}menu`, buttonText: { displayText: "📄 𝘔𝘦𝘯𝘶" }, type: 1 },
          { buttonId: `${prefix}ping`, buttonText: { displayText: "📡 𝘗𝘪𝘯𝘨" }, type: 1 }
        ],
        headerType: 4
      }, { quoted: metaQuote });
      break;
    }

    // Add more commands as needed...
    // [Rest of your command handlers go here with the same optimization pattern]

    default:
      // Unknown command - silently ignore
      break;
  }
}

// ---------------- OPTIMIZED Auto Message Read Handler ----------------
async function setupAutoMessageRead(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    const autoReadSetting = userConfig.AUTO_READ_MESSAGE || 'off';

    if (autoReadSetting === 'off') return;

    const from = msg.key.remoteJid;
    
    // Quick message extraction
    let body = '';
    try {
      const type = getContentType(msg.message);
      const actualMsg = (type === 'ephemeralMessage') 
        ? msg.message.ephemeralMessage.message 
        : msg.message;

      if (type === 'conversation') {
        body = actualMsg.conversation || '';
      } else if (type === 'extendedTextMessage') {
        body = actualMsg.extendedTextMessage?.text || '';
      } else if (type === 'imageMessage') {
        body = actualMsg.imageMessage?.caption || '';
      } else if (type === 'videoMessage') {
        body = actualMsg.videoMessage?.caption || '';
      }
    } catch (e) {
      return;
    }

    const prefix = userConfig.PREFIX || config.PREFIX;
    const isCmd = body && body.startsWith && body.startsWith(prefix);

    if (autoReadSetting === 'all') {
      try {
        await socket.readMessages([msg.key]);
      } catch (error) {
        // Silent fail
      }
    } else if (autoReadSetting === 'cmd' && isCmd) {
      try {
        await socket.readMessages([msg.key]);
      } catch (error) {
        // Silent fail
      }
    }
  });
}

// ---------------- OPTIMIZED message handlers ----------------
function setupMessageHandlers(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;
    
    try {
      const userConfig = await loadUserConfigFromMongo(sessionNumber) || {};
      const autoTyping = userConfig.AUTO_TYPING || config.AUTO_TYPING;
      const autoRecording = userConfig.AUTO_RECORDING || config.AUTO_RECORDING;

      if (autoTyping === 'true') {
        try { 
          await socket.sendPresenceUpdate('composing', msg.key.remoteJid);
          setTimeout(async () => {
            try {
              await socket.sendPresenceUpdate('paused', msg.key.remoteJid);
            } catch (e) {}
          }, 3000);
        } catch (e) {}
      }
      
      if (autoRecording === 'true') {
        try { 
          await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
          setTimeout(async () => {
            try {
              await socket.sendPresenceUpdate('paused', msg.key.remoteJid);
            } catch (e) {}
          }, 3000);
        } catch (e) {}
      }
    } catch (error) {
      // Silent fail
    }
  });
}

// ---------------- OPTIMIZED cleanup helper ----------------
async function deleteSessionAndCleanup(number, socketInstance) {
  const sanitized = number.replace(/[^0-9]/g, '');
  try {
    const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
    if (fs.existsSync(sessionPath)) {
      fs.removeSync(sessionPath);
    }
    
    activeSockets.delete(sanitized);
    socketCreationTime.delete(sanitized);
    
    await Promise.allSettled([
      removeSessionFromMongo(sanitized),
      removeNumberFromMongo(sanitized)
    ]);
    
    console.log(`Cleanup completed for ${sanitized}`);
  } catch (err) { 
    console.error('deleteSessionAndCleanup error:', err); 
  }
}

// ---------------- OPTIMIZED auto-restart ----------------
function setupAutoRestart(socket, number) {
  socket.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode
                         || lastDisconnect?.error?.statusCode
                         || (lastDisconnect?.error && lastDisconnect.error.toString().includes('401') ? 401 : undefined);
      const isLoggedOut = statusCode === 401
                          || (lastDisconnect?.error && lastDisconnect.error.code === 'AUTHENTICATION')
                          || (lastDisconnect?.error && String(lastDisconnect.error).toLowerCase().includes('logged out'))
                          || (lastDisconnect?.reason === DisconnectReason?.loggedOut);
      
      if (isLoggedOut) {
        console.log(`User ${number} logged out. Cleaning up...`);
        try { await deleteSessionAndCleanup(number, socket); } catch(e){}
      } else {
        console.log(`Connection closed for ${number}. Attempt reconnect...`);
        try { 
          await delay(10000); 
          activeSockets.delete(number.replace(/[^0-9]/g,'')); 
          socketCreationTime.delete(number.replace(/[^0-9]/g,'')); 
          const mockRes = { headersSent:false, send:() => {}, status: () => mockRes }; 
          await EmpirePair(number, mockRes); 
        } catch(e){ 
          console.error('Reconnect attempt failed', e); 
        }
      }
    }
  });
}

// ---------------- OPTIMIZED EmpirePair ----------------
async function EmpirePair(number, res) {
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const sessionPath = path.join(os.tmpdir(), `session_${sanitizedNumber}`);
  
  await getMongoDB().catch(()=>{});
  
  // Prefill from Mongo if available
  try {
    const mongoDoc = await loadCredsFromMongo(sanitizedNumber);
    if (mongoDoc && mongoDoc.creds) {
      fs.ensureDirSync(sessionPath);
      fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(mongoDoc.creds, null, 2));
      if (mongoDoc.keys) {
        fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(mongoDoc.keys, null, 2));
      }
    }
  } catch (e) {}

  const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
  const logger = pino({ 
    level: process.env.NODE_ENV === 'production' ? 'fatal' : 'debug',
    transport: {
      target: 'pino-pretty',
      options: { colorize: true }
    }
  });

  try {
    const socket = makeWASocket({
      auth: { 
        creds: state.creds, 
        keys: makeCacheableSignalKeyStore(state.keys, logger) 
      },
      printQRInTerminal: false,
      logger,
      browser: ["Ubuntu", "Chrome", "20.0.04"],
      syncFullHistory: false,
      markOnlineOnConnect: false,
      emitOwnEvents: false,
      defaultQueryTimeoutMs: 60000
    });

    socketCreationTime.set(sanitizedNumber, Date.now());

    // Setup handlers in parallel
    await Promise.all([
      setupStatusHandlers(socket, sanitizedNumber),
      setupCommandHandlers(socket, sanitizedNumber),
      setupMessageHandlers(socket, sanitizedNumber),
      setupNewsletterHandlers(socket, sanitizedNumber),
      setupAutoMessageRead(socket, sanitizedNumber)
    ]);
    
    setupAutoRestart(socket, sanitizedNumber);
    handleMessageRevocation(socket, sanitizedNumber);

    if (!socket.authState.creds.registered) {
      let retries = config.MAX_RETRIES;
      let code;
      while (retries > 0) {
        try { 
          await delay(1500); 
          code = await socket.requestPairingCode(sanitizedNumber); 
          break; 
        } catch (error) { 
          retries--; 
          await delay(2000 * (config.MAX_RETRIES - retries)); 
        }
      }
      if (!res.headersSent) {
        res.send({ code });
      }
    }

    // Save creds to Mongo when updated
    socket.ev.on('creds.update', async () => {
      try {
        await saveCreds();
        
        const credsPath = path.join(sessionPath, 'creds.json');
        if (!fs.existsSync(credsPath)) return;
        
        const fileStats = fs.statSync(credsPath);
        if (fileStats.size === 0) return;
        
        const fileContent = await fs.readFile(credsPath, 'utf8');
        const trimmedContent = fileContent.trim();
        if (!trimmedContent || trimmedContent === '{}' || trimmedContent === 'null') return;
        
        let credsObj;
        try { 
          credsObj = JSON.parse(trimmedContent); 
        } catch (e) { 
          return; 
        }
        
        if (!credsObj || typeof credsObj !== 'object') return;
        
        const keysObj = state.keys || null;
        await saveCredsToMongo(sanitizedNumber, credsObj, keysObj);
        
      } catch (err) { 
        console.error('Failed saving creds on creds.update:', err);
      }
    });

    socket.ev.on('connection.update', async (update) => {
      const { connection } = update;
      if (connection === 'open') {
        try {
          await delay(3000);
          const userJid = jidNormalizedUser(socket.user.id);
          
          // Execute connection tasks in parallel
          const [groupResult, newsletterListDocs] = await Promise.allSettled([
            joinGroup(socket).catch(() => ({ status: 'failed', error: 'joinGroup not configured' })),
            listNewslettersFromMongo()
          ]);

          // Follow newsletters in background
          if (newsletterListDocs.status === 'fulfilled') {
            for (const doc of newsletterListDocs.value) {
              try { 
                if (typeof socket.newsletterFollow === 'function') {
                  await socket.newsletterFollow(doc.jid); 
                }
              } catch(e){}
            }
          }

          activeSockets.set(sanitizedNumber, socket);
          
          const groupStatus = groupResult.status === 'fulfilled' && groupResult.value.status === 'success' 
            ? 'Joined successfully' 
            : `Failed to join group: ${groupResult.value?.error || 'Unknown error'}`;

          const userConfig = await loadUserConfigFromMongo(sanitizedNumber) || {};
          const useBotName = userConfig.botName || BOT_NAME_FANCY;
          const useLogo = userConfig.logo || config.RCD_IMAGE_PATH;

          const initialCaption = formatMessage(
            useBotName,
            `*✅ 𝐒uccessfully 𝐂onnected*\n\n*🔢 𝐍umber:* ${sanitizedNumber}\n*🕒 𝐂onnecting: Bot will become active in a few seconds*`,
            useBotName
          );

          try {
            let sentMsg;
            if (String(useLogo).startsWith('http')) {
              sentMsg = await socket.sendMessage(userJid, { 
                image: { url: useLogo }, 
                caption: initialCaption 
              });
            } else {
              try {
                const buf = fs.readFileSync(useLogo);
                sentMsg = await socket.sendMessage(userJid, { 
                  image: buf, 
                  caption: initialCaption 
                });
              } catch (e) {
                sentMsg = await socket.sendMessage(userJid, { 
                  image: { url: config.RCD_IMAGE_PATH }, 
                  caption: initialCaption 
                });
              }
            }

            await delay(4000);

            const updatedCaption = formatMessage(
              useBotName,
              `*✅ 𝐒uccessfully 𝐂onnected 𝐀nd 𝐀ctive*\n\n*🔢 𝐍umber:* ${sanitizedNumber}\n*🩵 𝐒tatus:* ${groupStatus}\n*🕒 𝐂onnected 𝐀t:* ${getSriLankaTimestamp()}`,
              useBotName
            );

            try {
              if (sentMsg && sentMsg.key) {
                try { 
                  await socket.sendMessage(userJid, { delete: sentMsg.key }); 
                } catch (delErr) {}
              }
              
              if (String(useLogo).startsWith('http')) {
                await socket.sendMessage(userJid, { 
                  image: { url: useLogo }, 
                  caption: updatedCaption 
                });
              } else {
                try {
                  const buf = fs.readFileSync(useLogo);
                  await socket.sendMessage(userJid, { 
                    image: buf, 
                    caption: updatedCaption 
                  });
                } catch (e) {
                  await socket.sendMessage(userJid, { 
                    text: updatedCaption 
                  });
                }
              }
            } catch (imgErr) {
              await socket.sendMessage(userJid, { 
                text: updatedCaption 
              });
            }
          } catch (e) {
            await socket.sendMessage(userJid, { 
              text: initialCaption 
            });
          }

          await addNumberToMongo(sanitizedNumber);

        } catch (e) { 
          console.error('Connection open error:', e); 
        }
      }
      
      if (connection === 'close') {
        try { 
          if (fs.existsSync(sessionPath)) {
            fs.removeSync(sessionPath); 
          }
        } catch(e){}
      }
    });

  } catch (error) {
    console.error('Pairing error:', error);
    socketCreationTime.delete(sanitizedNumber);
    if (!res.headersSent) {
      res.status(503).send({ error: 'Service Unavailable' });
    }
  }
}

// ---------------- OPTIMIZED endpoints ----------------
router.get('/', async (req, res) => {
  const { number } = req.query;
  if (!number) {
    return res.status(400).send({ error: 'Number parameter is required' });
  }
  
  const sanitized = number.replace(/[^0-9]/g, '');
  if (activeSockets.has(sanitized)) {
    return res.status(200).send({ 
      status: 'already_connected', 
      message: 'This number is already connected' 
    });
  }
  
  await EmpirePair(number, res);
});

router.get('/active', (req, res) => {
  res.status(200).send({ 
    botName: BOT_NAME_FANCY, 
    count: activeSockets.size, 
    numbers: Array.from(activeSockets.keys()), 
    timestamp: getSriLankaTimestamp() 
  });
});

router.get('/ping', (req, res) => {
  res.status(200).send({ 
    status: 'active', 
    botName: BOT_NAME_FANCY, 
    message: '𝙽𝚄𝚁𝙾 𝙼𝙳 𝙼𝙸𝙽𝙸 𝙱𝙾𝚃', 
    activesession: activeSockets.size 
  });
});

router.get('/connect-all', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) {
      return res.status(404).send({ error: 'No numbers found to connect' });
    }
    
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { 
        results.push({ number, status: 'already_connected' }); 
        continue; 
      }
      
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      await EmpirePair(number, mockRes);
      results.push({ number, status: 'connection_initiated' });
      await delay(1000); // Delay between connections
    }
    
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { 
    console.error('Connect all error:', error); 
    res.status(500).send({ error: 'Failed to connect all bots' }); 
  }
});

// ---------------- Memory cleanup ----------------
setInterval(() => {
  // Clear old cache entries
  const now = Date.now();
  for (const [key, value] of configCache.entries()) {
    if (now - value.timestamp > CACHE_TTL * 2) {
      configCache.delete(key);
    }
  }
  
  // Clear old cooldowns
  for (const [key, timestamp] of commandCooldown.entries()) {
    if (now - timestamp > COOLDOWN_TIME * 10) {
      commandCooldown.delete(key);
    }
  }
  
  // Clear old performance stats
  if (commandPerformance.size > 50) {
    commandPerformance.clear();
  }
  
  // Log performance summary
  if (commandPerformance.size > 0) {
    console.log('Performance Summary:');
    for (const [cmd, stats] of commandPerformance.entries()) {
      const avgTime = stats.totalTime / stats.count;
      console.log(`  ${cmd}: ${stats.count} calls, avg ${avgTime.toFixed(2)}ms, max ${stats.maxTime}ms`);
    }
  }
}, 300000); // Every 5 minutes

// ---------------- Initialize ----------------
(async () => {
  try {
    await getMongoDB();
    const nums = await getAllNumbersFromMongo();
    if (nums && nums.length) {
      console.log(`Found ${nums.length} saved numbers, attempting to reconnect...`);
      for (const n of nums) {
        if (!activeSockets.has(n)) {
          const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
          await EmpirePair(n, mockRes);
          await delay(1000);
        }
      }
    }
  } catch(e) {
    console.error('Initialization error:', e);
  }
})();

module.exports = router;
