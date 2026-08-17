#!/usr/bin/env node

/**
 * Soly Autonomous Job Scraper & Ingestion Engine (ADR-030)
 *
 * Standalone worker executed via GitHub Actions (or locally).
 * 1. Fetches Togo job boards (Jina AI Reader + Direct Cheerio Fallback)
 * 2. Sequential AI parsing cascade (Gemini 2.5 Flash-Lite -> Groq Llama 3.3 70B)
 * 3. Exponential backoff on 429 (Rate Limits)
 * 4. Pushes validated jobs to Soly API: POST /api/v1/jobs/ingest
 * 5. Notifies Telegram Admin on unexpected failure
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

// Auto-load .env.local / .env in local development
const envPaths = [".env.local", ".env"];
for (const envFile of envPaths) {
  const fullPath = path.resolve(process.cwd(), envFile);
  if (fs.existsSync(fullPath)) {
    const envContent = fs.readFileSync(fullPath, "utf-8");
    for (const line of envContent.split("\n")) {
      const trimmed = line.trim();
      if (trimmed && !trimmed.startsWith("#")) {
        const eqIdx = trimmed.indexOf("=");
        if (eqIdx > 0) {
          const key = trimmed.slice(0, eqIdx).trim();
          let val = trimmed.slice(eqIdx + 1).trim();
          if (
            (val.startsWith('"') && val.endsWith('"')) ||
            (val.startsWith("'") && val.endsWith("'"))
          ) {
            val = val.slice(1, -1);
          }
          if (!process.env[key]) {
            process.env[key] = val;
          }
        }
      }
    }
  }
}

const isDryRun = process.argv.includes("--dry-run");

const CONFIG = {
  appUrl: (process.env.APP_URL || "https://soly.work").replace(/\/$/, ""),
  scraperSecret:
    process.env.SCRAPER_SECRET ||
    process.env.SYNC_KEY ||
    process.env.CRON_SECRET ||
    process.env.INGESTION_KEY ||
    "",
  geminiApiKey: process.env.GEMINI_API_KEY || "",
  geminiApiKeyFallback: process.env.GEMINI_API_KEY_FALLBACK || "",
  groqApiKey: process.env.GROQ_API_KEY || "",
  jinaApiKey: process.env.JINA_API_KEY || "",
  telegramBotToken: process.env.TELEGRAM_BOT_TOKEN || "",
  telegramAdminChatId:
    process.env.TELEGRAM_ADMIN_CHAT_ID || process.env.TELEGRAM_CHAT_ID || "",
  geminiModel: process.env.GEMINI_MODEL || "gemini-flash-latest",
  geminiFallbackModel:
    process.env.GEMINI_FALLBACK_MODEL || "gemini-flash-lite-latest",
  groqModel: process.env.GROQ_MODEL || "llama-3.3-70b-versatile",
  maxUrlsPerSource: 5,
  delayBetweenAiCallsMs: 3500,
};

// Target Job Sources (Togo & Regional)
const SOURCES = [
  {
    name: "Jobrelais Togo",
    url: "https://www.jobrelais.com/opportunities/jobs",
    type: "html",
  },
  {
    name: "Emploi Togo",
    url: "https://www.emploitogo.info/offres-emploi/",
    type: "html",
  },
  {
    name: "ANPE Togo",
    url: "https://anpetogo.org/offres-demploi/",
    type: "html",
  },
];

const JSON_PREFIX_REGEX = /^```json\s*/i;
const JSON_SUFFIX_REGEX = /\s*```$/i;
const WWW_PREFIX_REGEX = /^www\./;

const STOP_MARKERS_REGEX = [
  /\n\s*Lire aussi\s*:/i,
  /\n\s*######\s*\[EmploiTogo/i,
  /\n\s*Partager cette offre\s*:/i,
  /\n\s*À propos de l'auteur/i,
  /\n\s*Autres offres similaires/i,
];

// Top-level regex constants for strict URL validation
const JOBRELAIS_JOB_REGEX =
  /^\/opportunities\/(jobs|internship|call-for-tenders|competitions)\/[a-f0-9]{8,}-[a-z0-9-]+$/i;
const EMPLOITOGO_EXCLUSION_REGEX =
  /^\/(offres-emploi|espace-recruteurs|publier-une-offre|publier-une-offre-demploi|category|tag|author|page|a-propos|contact|mentions-legales|politique|feed|comments)(\/.*)?$/i;
const EMPLOITOGO_JOB_REGEX =
  /\/(.*(recrute|recrutement|charge-de|avis-dappel|poste|assistant|directeur|ingenieur|commercial|stage|conducteur|responsable|comptable|juriste|consultant).*|\d{2}-\d{2}-\d{4})\/?$/i;
const ANPE_ROOT_REGEX = /^\/offres-demploi\/?$/i;
const ANPE_NUM_PREFIX_REGEX = /\/\d+-/;
const ANPE_JOB_REGEX_1 = /\/offres-demploi\/[a-z0-9-]+/i;
const ANPE_JOB_REGEX_2 = /\/offres-emploi\/\d+-[a-z0-9-]+/i;

// Pre-AI Structural Filter: verify page contains mandatory job announcement sections
const JOB_MARKERS_REGEX = [
  /missions?\s*(principales?|du poste)?/i,
  /profil\s*(recherch[ée]|du candidat)?/i,
  /conditions?\s*(d'embauche|du contrat)?/i,
  /candidature|comment postuler|pour postuler/i,
  /date limite/i,
  /niveau\s*(bac|licence|master|etudes?)/i,
  /comp[ée]tences?\s*(requises?|cl[ée]s?)?/i,
  /contrat\s*:\s*(cdi|cdd|stage|freelance)/i,
  /dossier de candidature/i,
  /pi[èe]ces?\s*[àa]\s*fournir/i,
  /avis de recrutement/i,
  /recrute/i,
];

// Helper: Check if URL is a strictly valid job detail URL for each specific source
function isStrictJobUrl(rawUrl, baseUrl) {
  if (!rawUrl) {
    return false;
  }
  try {
    const parsed = new URL(rawUrl, baseUrl);
    parsed.hash = "";
    const path = parsed.pathname;
    const host = parsed.hostname.replace(WWW_PREFIX_REGEX, "");

    // 1. Jobrelais Togo
    if (host.includes("jobrelais.com")) {
      // STRICT: Must be /opportunities/(jobs|internship|call-for-tenders|competitions)/[hexId]-[slug]
      return JOBRELAIS_JOB_REGEX.test(path);
    }

    // 2. Emploi Togo
    if (host.includes("emploitogo.info")) {
      // Must not be listing root, taxonomies, feeds, or standard static pages
      if (path === "/" || EMPLOITOGO_EXCLUSION_REGEX.test(path)) {
        return false;
      }
      // Must match job slug keywords or date suffix
      return EMPLOITOGO_JOB_REGEX.test(path);
    }

    // 3. ANPE Togo
    if (host.includes("anpetogo.org")) {
      if (
        path === "/" ||
        ANPE_ROOT_REGEX.test(path) ||
        (path.includes("offres-emploi") && !ANPE_NUM_PREFIX_REGEX.test(path))
      ) {
        return false;
      }
      return ANPE_JOB_REGEX_1.test(path) || ANPE_JOB_REGEX_2.test(path);
    }

    return false;
  } catch {
    return false;
  }
}

// Helper: Pre-AI filter to verify page content contains real job posting structure
function isValidJobPageContent(text) {
  if (!text || text.length < 250) {
    return false;
  }
  const lower = text.toLowerCase();

  // Reject image captions and platform marketing
  if (
    lower.includes("image description and analysis") ||
    lower.includes("logo d'oxfam") ||
    lower.includes("nos offres d'abonnements") ||
    (lower.includes(
      "cabinet de recrutement, conseils et solutions pour votre carrière"
    ) &&
      text.length < 600)
  ) {
    return false;
  }

  let matches = 0;
  for (const m of JOB_MARKERS_REGEX) {
    if (m.test(lower)) {
      matches++;
    }
  }

  return matches >= 1 && text.length >= 350;
}

// Helper: Post-AI filter to verify extracted JSON integrity
function isValidExtractedJob(parsed) {
  if (!parsed || typeof parsed !== "object") {
    return false;
  }

  const title = (parsed.title || "").trim();
  const company = (parsed.company || "").trim();
  const description = (parsed.description || "").trim();
  const lowerTitle = title.toLowerCase();
  const lowerCompany = company.toLowerCase();

  // 1. Title checks
  if (title.length < 4) {
    return false;
  }
  const invalidTitleKeywords = [
    "image analysis",
    "image description",
    "non spécifié",
    "non specifie",
    "not found",
    "aucune offre",
    "404",
    "à propos",
    "a propos",
    "about us",
    "services",
    "abonnement",
    "connexion",
    "inscription",
    "présentation",
    "presentation",
    "accueil",
    "opportunités",
    "opportunites",
  ];
  if (
    invalidTitleKeywords.some((k) => lowerTitle.includes(k) || lowerTitle === k)
  ) {
    return false;
  }

  // 2. Company checks
  if (company.length < 2) {
    return false;
  }
  if (
    lowerCompany === "non spécifié" ||
    lowerCompany === "unknown" ||
    lowerCompany === "inconnu" ||
    lowerCompany === "not found"
  ) {
    return false;
  }

  // 3. Platform self-description check
  if (
    (lowerCompany === "jobrelais" ||
      lowerCompany === "jobrelais sarl" ||
      lowerCompany === "emploitogo") &&
    !lowerTitle.includes("recrute")
  ) {
    return false;
  }

  // 4. Description checks
  if (description.length < 200) {
    return false;
  }
  if (
    description.toLowerCase().includes("image description and analysis") ||
    description.toLowerCase().includes("logo d'")
  ) {
    return false;
  }

  return true;
}

// Helper: Sleep
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Helper: Generate Deterministic Fingerprint
function generateFingerprint({ title, company, location }) {
  const normTitle = (title || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]/g, "");
  const normCompany = (company || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]/g, "");
  const normLocation = (location || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]/g, "");
  return crypto
    .createHash("sha256")
    .update(`${normTitle}|${normCompany}|${normLocation}`)
    .digest("hex");
}

function normalizeContractType(val) {
  if (typeof val !== "string" || !val.trim()) {
    return;
  }
  const s = val.toLowerCase().trim();
  if (
    s.includes("cdi") ||
    s.includes("indéterminée") ||
    s.includes("indeterminee")
  ) {
    return "CDI";
  }
  if (
    s.includes("cdd") ||
    s.includes("déterminée") ||
    s.includes("determinee")
  ) {
    return "CDD";
  }
  if (s.includes("stage") || s.includes("intern")) {
    return "Stage";
  }
  if (
    s.includes("freelance") ||
    s.includes("consultan") ||
    s.includes("prestation") ||
    s.includes("marché")
  ) {
    return "Freelance";
  }
  if (s.includes("alternan") || s.includes("apprentis")) {
    return "Alternance";
  }
  return "Autre";
}

function normalizeWorkMode(val) {
  if (typeof val !== "string" || !val.trim()) {
    return;
  }
  const s = val.toLowerCase().trim();
  if (
    s.includes("remote") ||
    s.includes("télétravail") ||
    s.includes("teletravail") ||
    s.includes("distance")
  ) {
    return "remote";
  }
  if (s.includes("hybrid") || s.includes("hybride")) {
    return "hybrid";
  }
  return "office";
}

// Helper: Send Telegram Admin Alert
async function sendAdminAlert(message) {
  if (!(CONFIG.telegramBotToken && CONFIG.telegramAdminChatId)) {
    console.warn("[Scraper Alert] Telegram Admin credentials not configured.");
    return;
  }
  try {
    await fetch(
      `https://api.telegram.org/bot${CONFIG.telegramBotToken}/sendMessage`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: CONFIG.telegramAdminChatId,
          text: `🚨 <b>Soly Scraper Alert</b>\n\n${message}`,
          parse_mode: "HTML",
        }),
      }
    );
  } catch (err) {
    console.error("[Scraper Alert] Failed to send Telegram alert:", err);
  }
}

// Helper: Fetch with Jina AI Reader + Direct Fallback
async function fetchPageText(url) {
  console.log(`[Crawler] Fetching: ${url}`);

  // 1. Try Jina Reader
  try {
    const jinaHeaders = {
      "x-respond-with": "markdown",
      "User-Agent": "Mozilla/5.0 (compatible; SolyBot/2.0; +https://soly.work)",
    };
    if (CONFIG.jinaApiKey) {
      jinaHeaders.Authorization = `Bearer ${CONFIG.jinaApiKey}`;
    }

    const jinaRes = await fetch(`https://r.jina.ai/${url}`, {
      headers: jinaHeaders,
      signal: AbortSignal.timeout(25_000),
    });

    if (jinaRes.ok) {
      const text = await jinaRes.text();
      const lower = text.toLowerCase();
      const isError =
        text.length < 150 ||
        lower.includes("403 forbidden") ||
        lower.includes("blocked by cloudflare") ||
        lower.includes("rate limited");

      if (!isError) {
        console.log(`[Crawler] ✅ Jina AI success (${text.length} chars)`);
        return text;
      }
    }
  } catch (err) {
    console.log(
      `[Crawler] Jina AI fetch failed for ${url}: ${err.message}. Falling back to direct fetch.`
    );
  }

  // 2. Direct Fallback
  try {
    const directRes = await fetch(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
      },
      signal: AbortSignal.timeout(15_000),
    });

    if (directRes.ok) {
      const html = await directRes.text();
      console.log(`[Crawler] ✅ Direct fetch success (${html.length} chars)`);
      return html;
    }
  } catch (err) {
    console.error(`[Crawler] ❌ Direct fetch failed for ${url}:`, err.message);
  }

  return "";
}

// Helper: Extract Clean Full Markdown announcement
function extractCleanFullDescription(rawPageText, aiDescription) {
  if (!rawPageText) {
    return aiDescription || "";
  }

  let cleaned = rawPageText;

  // 1. If fetched via Jina, strip Jina meta headers
  const jinaIdx = cleaned.indexOf("Markdown Content:");
  if (jinaIdx !== -1) {
    cleaned = cleaned.slice(jinaIdx + "Markdown Content:".length).trim();
  }

  // 2. Strip standard bottom widgets/footers
  for (const marker of STOP_MARKERS_REGEX) {
    const match = cleaned.match(marker);
    if (match && match.index !== undefined && match.index > 250) {
      cleaned = cleaned.slice(0, match.index).trim();
    }
  }

  // 3. If the cleaned markdown is rich and contains substantive detail, preserve it
  if (cleaned.length > 300) {
    return cleaned;
  }

  return aiDescription || cleaned;
}

// Helper: Extract URLs matching job patterns
function extractJobUrls(content, baseUrl) {
  const found = new Set();
  const baseHostname = new URL(baseUrl).hostname.replace(WWW_PREFIX_REGEX, "");

  function processCandidateUrl(rawUrl) {
    if (!rawUrl) {
      return;
    }
    try {
      const parsed = new URL(rawUrl, baseUrl);
      parsed.hash = "";
      const cleaned = parsed.href;
      const candidateHostname = parsed.hostname.replace(WWW_PREFIX_REGEX, "");

      // STRICT DOMAIN ISOLATION: Never crawl outbound external links
      if (candidateHostname !== baseHostname) {
        return;
      }

      // Check strict source-specific job patterns
      if (isStrictJobUrl(cleaned, baseUrl)) {
        found.add(cleaned);
      }
    } catch {
      // Ignore invalid URLs
    }
  }

  // 1. Regex parsing for HTML links: href="..."
  const urlRegex = /href=["']([^"']+)["']/gi;
  for (const match of content.matchAll(urlRegex)) {
    processCandidateUrl(match[1]);
  }

  // 2. Regex parsing for Markdown links: [title](url)
  const mdRegex = /\[([^\]]*?)\]\((https?:\/\/[^\s)]+)\)/g;
  for (const match of content.matchAll(mdRegex)) {
    processCandidateUrl(match[2]);
  }

  // 3. Regex parsing for raw URLs
  const rawUrlRegex = /(https?:\/\/[^\s)>]+)/g;
  for (const match of content.matchAll(rawUrlRegex)) {
    processCandidateUrl(match[1]);
  }

  return Array.from(found).slice(0, CONFIG.maxUrlsPerSource);
}

// Single call to Google Gemini endpoint
async function callGeminiApi(apiKey, model, pageText, sourceUrl) {
  if (!apiKey) {
    return null;
  }

  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;

  const prompt = `Tu es un bot expert en extraction d'offres d'emploi en Afrique de l'Ouest (Togo).
Extrais les informations de l'offre d'emploi suivante.
Retourne STRICTEMENT un objet JSON valide avec cette structure (sans backticks markdown \`\`\`json) :
{
  "title": "Titre du poste exact",
  "company": "Nom de l'entreprise",
  "location": "Lomé, Togo (ou Togo / Remote)",
  "description": "Description exhaustive et détaillée avec missions et profil (Markdown)",
  "summary": "Résumé de 2-3 phrases en français",
  "contractType": "CDI" | "CDD" | "Stage" | "Freelance" | "Alternance" | "Autre",
  "workMode": "office" | "hybrid" | "remote",
  "sector": "Tech" | "Finance" | "Commercial" | "RH" | "Santé" | "Marketing" | "Logistique" | "Éducation" | "BTP" | "Juridique" | "Agriculture" | "Hôtellerie" | "ONG" | "Design" | "Autre",
  "salary": "Salaire explicite ou null",
  "skills": ["Compétence 1", "Compétence 2"],
  "applyUrl": "URL de candidature ou email ou téléphone",
  "howToApply": "Instructions précises de candidature"
}

Texte source (URL: ${sourceUrl}) :
${pageText.slice(0, 20_000)}`;

  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: {
          responseMimeType: "application/json",
          temperature: 0.1,
        },
      }),
      signal: AbortSignal.timeout(20_000),
    });

    if (res.status === 429) {
      return { isQuotaError: true };
    }

    if (!res.ok) {
      throw new Error(`Gemini HTTP ${res.status}: ${await res.text()}`);
    }

    const data = await res.json();
    const rawText = data.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!rawText) {
      return null;
    }

    const cleanJson = rawText
      .replace(JSON_PREFIX_REGEX, "")
      .replace(JSON_SUFFIX_REGEX, "")
      .trim();
    return JSON.parse(cleanJson);
  } catch (err) {
    console.warn(`[Gemini - ${model}] Extraction error:`, err.message);
    return null;
  }
}

// AI Extraction: Call Primary Gemini -> Secondary Fallback Gemini
async function parseWithGemini(pageText, sourceUrl) {
  // 1. Try Primary Gemini Key
  if (CONFIG.geminiApiKey) {
    const result = await callGeminiApi(
      CONFIG.geminiApiKey,
      CONFIG.geminiModel,
      pageText,
      sourceUrl
    );
    if (result && !result.isQuotaError) {
      return result;
    }
    if (result?.isQuotaError) {
      console.warn(
        "[Gemini] ⚠️ Primary key quota limit reached (429). Switching to fallback key..."
      );
    }
  }

  // 2. Try Fallback Gemini Key
  if (CONFIG.geminiApiKeyFallback) {
    const result = await callGeminiApi(
      CONFIG.geminiApiKeyFallback,
      CONFIG.geminiFallbackModel,
      pageText,
      sourceUrl
    );
    if (result && !result.isQuotaError) {
      console.log("[Gemini] ✅ Fallback Gemini key success.");
      return result;
    }
  }

  return null;
}

// AI Extraction: Call Groq (Llama 3.3 70B or Llama 3.1 8B)
async function parseWithGroq(
  pageText,
  sourceUrl,
  modelName = CONFIG.groqModel
) {
  if (!CONFIG.groqApiKey) {
    return null;
  }

  const endpoint = "https://api.groq.com/openai/v1/chat/completions";

  const prompt = `Tu es un expert RH. Extrais l'offre d'emploi sous forme d'un objet JSON strict avec :
- title: Titre exact du poste
- company: Nom de l'entreprise ou 'Entreprise confidentielle'
- location: Ville, Togo (ou Remote)
- description: Corps exhaustif de l'annonce
- summary: Résumé de 2-3 phrases en français
- contractType: 'CDI' | 'CDD' | 'Stage' | 'Freelance' | 'Alternance' | 'Autre'
- workMode: 'office' | 'hybrid' | 'remote'
- sector: Secteur d'activité
- skills: Liste de compétences requises
- salary: Salaire ou null
- applyUrl: Email, lien ou téléphone pour postuler
- howToApply: Consignes pour postuler

Texte (URL: ${sourceUrl}) :
${pageText.slice(0, 8000)}`;

  let retries = 0;
  const maxRetries = 2;

  while (retries <= maxRetries) {
    try {
      const res = await fetch(endpoint, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${CONFIG.groqApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          model: modelName,
          messages: [
            {
              role: "system",
              content:
                "Tu es un bot expert en extraction d'offres d'emploi. Réponds STRICTEMENT en JSON valide sans aucun formatage markdown.",
            },
            { role: "user", content: prompt },
          ],
          response_format: { type: "json_object" },
          temperature: 0.1,
        }),
        signal: AbortSignal.timeout(20_000),
      });

      if (res.status === 429) {
        retries++;
        const backoffMs = 2000 * retries;
        console.warn(
          `[Groq - ${modelName}] ⚠️ Rate limit 429. Waiting ${backoffMs}ms...`
        );
        await sleep(backoffMs);
        continue;
      }

      if (!res.ok) {
        throw new Error(`Groq HTTP ${res.status}: ${await res.text()}`);
      }

      const data = await res.json();
      const rawText = data.choices?.[0]?.message?.content;
      if (!rawText) {
        return null;
      }

      return JSON.parse(rawText);
    } catch (err) {
      if (retries >= maxRetries) {
        console.warn(
          `[Groq - ${modelName}] Failed after ${maxRetries} retries:`,
          err.message
        );
        return null;
      }
      retries++;
      await sleep(1500 * retries);
    }
  }

  return null;
}

// Ingestion to Soly API
async function ingestJobToSoly(jobData) {
  if (isDryRun) {
    console.log(
      `[DRY-RUN] Would ingest job: "${jobData.title}" at "${jobData.company}"`
    );
    return { success: true, isNew: true, jobId: "dry_run_id" };
  }

  const endpoint = `${CONFIG.appUrl}/api/v1/jobs/ingest?key=${encodeURIComponent(CONFIG.scraperSecret)}`;

  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-scraper-secret": CONFIG.scraperSecret,
        Authorization: `Bearer ${CONFIG.scraperSecret}`,
      },
      body: JSON.stringify({ job: jobData }),
      signal: AbortSignal.timeout(15_000),
    });

    const result = await res.json();
    if (!res.ok) {
      throw new Error(
        `Ingest API error ${res.status}: ${JSON.stringify(result)}`
      );
    }

    return result;
  } catch (err) {
    console.error(
      `[Ingest] ❌ Failed to ingest "${jobData.title}":`,
      err.message
    );
    throw err;
  }
}

// Sequential AI Extraction Cascade (Gemini Primary/Fallback -> Groq 70B -> Groq 8B)
async function extractJobWithAiCascade(detailText, jobUrl) {
  console.log("[AI] Parsing job with Gemini (Primary / Fallback)...");
  let parsed = await parseWithGemini(detailText, jobUrl);

  if (!(parsed?.title && parsed?.company)) {
    console.log(
      "[AI] ⚠️ Gemini failed or returned incomplete. Falling back to Groq 70B..."
    );
    parsed = await parseWithGroq(detailText, jobUrl, "llama-3.3-70b-versatile");
  }

  if (!(parsed?.title && parsed?.company)) {
    console.log(
      "[AI] ⚠️ Groq 70B failed or returned incomplete. Falling back to Groq 8B..."
    );
    parsed = await parseWithGroq(detailText, jobUrl, "llama-3.1-8b-instant");
  }

  return parsed;
}

// Process a single job candidate URL
async function processJobCandidate(jobUrl) {
  console.log(`\n[Process] URL: ${jobUrl}`);
  const detailText = await fetchPageText(jobUrl);
  if (!detailText || detailText.length < 250) {
    console.warn(`[Process] ⚠️ Page content too short or empty for ${jobUrl}`);
    return null;
  }

  // 1. Pre-AI Structural Gatekeeper
  if (!isValidJobPageContent(detailText)) {
    console.warn(
      `[Process] ⚠️ Discarded page without recruitment structure: ${jobUrl}`
    );
    return null;
  }

  // 2. Sequential AI Extraction Cascade
  const parsed = await extractJobWithAiCascade(detailText, jobUrl);
  if (!(parsed?.title && parsed?.company)) {
    console.warn(`[AI] ❌ All AI extractors failed for ${jobUrl}`);
    return null;
  }

  // 3. Post-AI Sémantique Gatekeeper
  if (!isValidExtractedJob(parsed)) {
    console.warn(
      `[AI] ⚠️ Discarded invalid job extraction: "${parsed.title}" at "${parsed.company}"`
    );
    return null;
  }

  const fingerprint = generateFingerprint({
    title: parsed.title,
    company: parsed.company,
    location: parsed.location || "Togo",
  });

  const fullDescription = extractCleanFullDescription(
    detailText,
    parsed.description
  );

  const payload = {
    title: parsed.title,
    company: parsed.company,
    location: parsed.location || "Togo",
    description: fullDescription || detailText.slice(0, 5000),
    summary: parsed.summary || parsed.title,
    contractType: normalizeContractType(parsed.contractType) || "CDI",
    workMode: normalizeWorkMode(parsed.workMode) || "office",
    sector: parsed.sector || "Autre",
    salary: parsed.salary || null,
    skills: Array.isArray(parsed.skills) ? parsed.skills : [],
    applyUrl: parsed.applyUrl || jobUrl,
    howToApply: parsed.howToApply || null,
    sourceUrl: jobUrl,
    fingerprint,
  };

  const ingestRes = await ingestJobToSoly(payload);
  return { payload, ingestRes };
}

// Main Runner
async function main() {
  console.log("==================================================");
  console.log("🚀 Soly Autonomous Scraper Engine Started (ADR-030)");
  console.log(`Timestamp : ${new Date().toISOString()}`);
  console.log(`Mode      : ${isDryRun ? "DRY-RUN" : "LIVE INGESTION"}`);
  console.log(`Target    : ${CONFIG.appUrl}`);
  console.log("==================================================\n");

  const startTime = Date.now();
  let totalDiscoveredUrls = 0;
  let totalExtracted = 0;
  let totalIngestedNew = 0;
  let totalDuplicates = 0;
  const errors = [];

  for (const source of SOURCES) {
    console.log(`\n--- Scanning Source: ${source.name} ---`);
    const pageContent = await fetchPageText(source.url);
    if (!pageContent) {
      console.warn(`[Scan] ⚠️ Could not fetch source: ${source.name}`);
      continue;
    }

    const jobUrls = extractJobUrls(pageContent, source.url);
    console.log(`[Scan] Found ${jobUrls.length} potential job URLs.`);
    totalDiscoveredUrls += jobUrls.length;

    for (const jobUrl of jobUrls) {
      try {
        const result = await processJobCandidate(jobUrl);
        if (result) {
          totalExtracted++;
          if (result.ingestRes?.isNew) {
            totalIngestedNew++;
            console.log(
              `[Ingest] ✨ New job created & queued: "${result.payload.title}" (ID: ${result.ingestRes.jobId})`
            );
          } else {
            totalDuplicates++;
            console.log(
              `[Ingest] 🔁 Duplicate skipped: "${result.payload.title}"`
            );
          }
        }
      } catch (err) {
        errors.push(`${jobUrl}: ${err.message}`);
      }

      // Pause between AI calls to respect RPM/TPM
      await sleep(CONFIG.delayBetweenAiCallsMs);
    }
  }

  const durationSec = Math.round((Date.now() - startTime) / 1000);
  console.log("\n==================================================");
  console.log("📊 Scraping Summary");
  console.log(`Duration         : ${durationSec}s`);
  console.log(`Discovered URLs  : ${totalDiscoveredUrls}`);
  console.log(`Extracted Jobs   : ${totalExtracted}`);
  console.log(`New Ingested     : ${totalIngestedNew}`);
  console.log(`Duplicates       : ${totalDuplicates}`);
  console.log(`Errors           : ${errors.length}`);
  console.log("==================================================");

  if (errors.length > 0 && totalIngestedNew === 0 && totalExtracted === 0) {
    const errorMsg = `Pipeline encountered ${errors.length} fatal errors:\n${errors.slice(0, 3).join("\n")}`;
    await sendAdminAlert(errorMsg);
    process.exit(1);
  }
}

main().catch(async (err) => {
  console.error("💥 Unhandled Scraper Failure:", err);
  await sendAdminAlert(`Critical scraper crash: ${err.message}`);
  process.exit(1);
});
