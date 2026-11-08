'use strict';

// Test data helpers for FAST (Fonların Anlık ve Sürekli Transferi) scenarios.
// All banks, IBANs and amounts are synthetic.

const crypto = require('node:crypto');

// Bank codes (IBAN positions 5-9). Weights roughly follow market size so the
// bank breakdown on the dashboard looks realistic. Enpara.com uses QNB's code.
const BANKS = [
  { code: '00010', name: 'Ziraat Bankası', weight: 14 },
  { code: '00064', name: 'İş Bankası', weight: 12 },
  { code: '00062', name: 'Garanti BBVA', weight: 11 },
  { code: '00067', name: 'Yapı Kredi', weight: 10 },
  { code: '00046', name: 'Akbank', weight: 10 },
  { code: '00015', name: 'Vakıfbank', weight: 9 },
  { code: '00012', name: 'Halkbank', weight: 7 },
  { code: '00111', name: 'QNB / Enpara.com', weight: 7 },
  { code: '00134', name: 'Denizbank', weight: 4 },
  { code: '00032', name: 'TEB', weight: 3 },
  { code: '00099', name: 'ING Bank', weight: 2 },
  { code: '00205', name: 'Kuveyt Türk', weight: 2 },
  { code: '00206', name: 'Türkiye Finans', weight: 1 },
  { code: '00203', name: 'Albaraka Türk', weight: 1 },
  { code: '00210', name: 'Vakıf Katılım', weight: 1 },
  { code: '00209', name: 'Ziraat Katılım', weight: 1 },
  { code: '00231', name: 'Papara', weight: 3 },
  { code: '00238', name: 'Hayhay', weight: 2 },
];
const TOTAL_WEIGHT = BANKS.reduce((sum, b) => sum + b.weight, 0);

function pickBank() {
  let r = Math.random() * TOTAL_WEIGHT;
  for (const bank of BANKS) {
    r -= bank.weight;
    if (r < 0) return bank;
  }
  return BANKS[0];
}
const FAILURE_REASONS = ['INSUFFICIENT_BALANCE', 'RECEIVER_ACCOUNT_CLOSED', 'DAILY_LIMIT_EXCEEDED', 'RECEIVER_BANK_TIMEOUT', 'FRAUD_SUSPECTED'];
const DESCRIPTIONS = ['Kira', 'Aidat', 'Borç ödemesi', 'Alışveriş', 'Harçlık', 'Fatura'];

const pick = (list) => list[Math.floor(Math.random() * list.length)];
const digits = (n) => Array.from({ length: n }, () => Math.floor(Math.random() * 10)).join('');

// Builds a TR IBAN with a valid ISO 13616 mod-97 check digit.
function trIban(bankCode) {
  const bban = `${bankCode}0${digits(16)}`;
  const numeric = `${bban}TR00`.replace(/[A-Z]/g, (c) => String(c.charCodeAt(0) - 55));
  const check = String(98n - (BigInt(numeric) % 97n)).padStart(2, '0');
  return `TR${check}${bban}`;
}

// Flow step: create a new transfer and keep it in the virtual user's context so
// fast_send and its outcome event share the same transaction_id.
function newTransfer(context, events, done) {
  const senderBank = pickBank().code;
  const receiverBank = pickBank().code;
  context.vars.transfer = {
    transaction_id: crypto.randomUUID(),
    amount: Math.round((10 + Math.random() * 49_990) * 100) / 100,
    currency: 'TRY',
    sender_bank: senderBank,
    receiver_bank: receiverBank,
    sender_iban: trIban(senderBank),
    receiver_iban: trIban(receiverBank),
    description: pick(DESCRIPTIONS),
    source: 'artillery',
  };
  return done();
}

// beforeRequest hook: build the body from the current transfer; the event name
// is taken from the URL (/events/<event>).
function fastBody(requestParams, context, ee, next) {
  const event = requestParams.url.split('/').pop();
  const body = { ...context.vars.transfer, sent_at: new Date().toISOString() };
  if (requestParams.url?.includes('/failed') || event === 'fast_send_failed') {
    body.failure_reason = pick(FAILURE_REASONS);
  }
  requestParams.json = body;
  return next();
}

function eventBody(requestParams, context, ee, next) {
  const body = { ...context.vars.transfer, sent_at: new Date().toISOString() };
  if (requestParams.url?.includes('/failed')) body.failure_reason = pick(FAILURE_REASONS);
  requestParams.json = body;
  return next();
}

function directionalBody(requestParams, context, ee, next) {
  const body = {
    transaction_id: `endpoint-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    amount: 10,
    currency: 'TRY',
    description: 'directional endpoint smoke test',
  };
  if (requestParams.url?.includes('/failed')) body.failure_reason = 'MOCK_FAILURE';
  requestParams.body = JSON.stringify(body);
  requestParams.headers = { ...(requestParams.headers ?? {}), 'content-type': 'application/json' };
  return next();
}

module.exports = { newTransfer, fastBody, eventBody, directionalBody };
