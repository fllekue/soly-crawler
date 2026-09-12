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

import fs from "node:fs";
import path from "node:path";
import { load } from "cheerio";
import TurndownService from "turndown";

const turndown = new TurndownService({
  headingStyle: "atx",
  bulletListMarker: "-",
  emDelimiter: "_",
});

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
  groqFallbackModel: process.env.GROQ_FALLBACK_MODEL || "llama-3.1-8b-instant",
  geminiTimeoutMs: 35_000,
  maxUrlsPerSource: 5,
  delayBetweenAiCallsMs: 3500,
};

// Default Job Sources (Fallback if API unavailable)
const DEFAULT_SOURCES = [
  {
    name: "Jobrelais Togo",
    url: "https://www.jobrelais.com/opportunities/jobs",
    type: "html",
  },
  {
    name: "Emploi Togo Info",
    url: "https://www.emploitogo.info/offres-emploi/",
    type: "html",
  },
  {
    name: "Emploi Togo (.com)",
    url: "https://emploitogo.com/listes-des-emplois/",
    type: "html",
  },
  {
    name: "ANPE Togo",
    url: "https://anpetogo.org/index.php/fr-fr/offres/offres-emploi",
    type: "html",
  },
  {
    name: "RMO Job Center Togo",
    url: "https://www.rmo-jobcenter.com/fr/togo/offres-emploi.html",
    type: "html",
  },
  {
    name: "Emploi.tg",
    url: "https://www.emploi.tg/recherche-jobs-togo",
    type: "html",
  },
];

function normalizeSourceListingUrl(rawUrl) {
  if (!rawUrl) {
    return rawUrl;
  }
  const url = rawUrl.trim();
  if (url.includes("anpetogo.org/offres-demploi")) {
    return "https://anpetogo.org/index.php/fr-fr/offres/offres-emploi";
  }
  if (url.includes("emploitogo.com/category/offres-d-emploi")) {
    return "https://emploitogo.com/listes-des-emplois/";
  }
  return url;
}

function mergeDefaultSources(dynamicSources) {
  const result = [...dynamicSources];
  for (const defSource of DEFAULT_SOURCES) {
    const defHost = new URL(defSource.url).hostname.replace(
      WWW_PREFIX_REGEX,
      ""
    );
    const exists = result.some(
      (ds) => new URL(ds.url).hostname.replace(WWW_PREFIX_REGEX, "") === defHost
    );
    if (!exists) {
      result.push(defSource);
    }
  }
  return result;
}

// Helper: Dynamically fetch active sources configured in Soly Back-Office
async function loadTargetSources() {
  if (!CONFIG.appUrl) {
    return DEFAULT_SOURCES;
  }

  try {
    const endpoint = `${CONFIG.appUrl}/api/v1/jobs/sources?key=${encodeURIComponent(CONFIG.scraperSecret)}`;
    const res = await fetch(endpoint, {
      headers: {
        "x-scraper-secret": CONFIG.scraperSecret,
        Authorization: `Bearer ${CONFIG.scraperSecret}`,
      },
      signal: AbortSignal.timeout(8000),
    });

    if (res.ok) {
      const data = await res.json();

      // Check if founder paused the crawler from Back-Office
      if (
        data.schedule?.isPaused &&
        !process.argv.includes("--force") &&
        !process.argv.includes("--dry-run")
      ) {
        console.log(
          "[Schedule] ⏸️ Scraper is paused in Soly Back-Office settings. Exiting cleanly."
        );
        process.exit(0);
      }

      if (Array.isArray(data.sources) && data.sources.length > 0) {
        const dynamicSources = data.sources.map((s) => ({
          name: s.name,
          url: normalizeSourceListingUrl(s.url),
          type: "html",
        }));

        const merged = mergeDefaultSources(dynamicSources);
        console.log(
          `[Sources] 🔗 Dynamically synchronized ${merged.length} active sources from Soly Back-Office.`
        );
        return merged;
      }
    }
  } catch (err) {
    console.warn(
      `[Sources] ⚠️ Could not fetch dynamic sources from ${CONFIG.appUrl} (${err.message}). Using fallback sources.`
    );
  }

  return DEFAULT_SOURCES;
}

const JSON_PREFIX_REGEX = /^```json\s*/i;
const JSON_SUFFIX_REGEX = /\s*```$/i;
const WWW_PREFIX_REGEX = /^www\./;

// Top-level regex constants for strict URL validation
const JOBRELAIS_JOB_REGEX =
  /^\/opportunities\/(jobs|internship|call-for-tenders|competitions)\/[a-z0-9-]+$/i;
const EMPLOITOGO_EXCLUSION_REGEX =
  /^\/(offres-emploi|espace-recruteurs|publier-une-offre|publier-une-offre-demploi|category|tag|author|page|a-propos|contact|mentions-legales|politique|feed|comments)(\/.*)?$/i;
const EMPLOITOGO_JOB_REGEX =
  /\/(.*(recrute|recrutement|charge-de|avis-dappel|poste|assistant|directeur|ingenieur|commercial|stage|conducteur|responsable|comptable|juriste|consultant).*|\d{2}-\d{2}-\d{4})\/?$/i;
const EMPLOITOGO_COM_DASH_REGEX = /-[a-z0-9]+/i;
const ANPE_ROOT_REGEX = /^\/(index\.php\/[a-z-]+)?\/?offres-d?emploi\/?$/i;
const ANPE_NUM_PREFIX_REGEX = /\/\d+-/;
const ANPE_JOB_REGEX_1 = /\/offres-demploi\/[a-z0-9-]+/i;
const ANPE_JOB_REGEX_2 = /\/offres-emploi\/\d+-[a-z0-9-]+/i;
const GENERIC_EXCLUSIONS_REGEX =
  /\.(jpg|jpeg|png|gif|svg|css|js|woff|pdf|rss|xml)$/i;
const GENERIC_BAD_PATHS_REGEX =
  /^\/(login|connexion|register|inscription|contact|a-propos|about|mentions-legales|politique|terms|tag|category|author|feed|page)\/?/i;

function isJobrelaisJobUrl(path) {
  if (
    path.includes("/pays/") ||
    path.endsWith("/create") ||
    path === "/opportunities/jobs" ||
    path === "/opportunities/jobs/" ||
    path === "/opportunities/internship" ||
    path === "/opportunities/competitions" ||
    path === "/opportunities/call-for-tenders" ||
    path === "/opportunities/scholarship"
  ) {
    return false;
  }
  return JOBRELAIS_JOB_REGEX.test(path);
}

function isEmploiTogoComJobUrl(path) {
  if (
    path === "/" ||
    path === "/listes-des-emplois" ||
    path === "/listes-des-emplois/" ||
    path.startsWith("/dashboard-page")
  ) {
    return false;
  }
  return (
    path.length > 5 &&
    (path.includes("/emploi") ||
      path.includes("/job") ||
      path.includes("/offres") ||
      EMPLOITOGO_COM_DASH_REGEX.test(path))
  );
}

function isAnpeJobUrl(path) {
  if (
    path === "/" ||
    ANPE_ROOT_REGEX.test(path) ||
    path.endsWith("/offres-emploi") ||
    path.endsWith("/offres-emploi/") ||
    !ANPE_NUM_PREFIX_REGEX.test(path)
  ) {
    return false;
  }
  return (
    ANPE_JOB_REGEX_1.test(path) ||
    ANPE_JOB_REGEX_2.test(path) ||
    path.includes("/offres-emploi/")
  );
}

function isEmploiTgJobUrl(path) {
  if (path === "/" || path.includes("/recherche-jobs")) {
    return false;
  }
  return (
    path.includes("/offre-emploi-") ||
    path.includes("/job-offer-") ||
    path.includes("/recrutement")
  );
}

const RMO_JOB_REGEX =
  /^\/fr\/togo\/offres-emploi\/[^/]+\/\d+-[a-z0-9-]+\.html$/i;
const RE_RMO_DUPLICATE_PREFIX = /^\/fr\/togo\/fr\/togo\//i;
const RE_BASE_TAG = /<base\s+[^>]*href=["']([^"']+)["']/i;

function isRmoJobUrl(path) {
  const normalized = path.replace(RE_RMO_DUPLICATE_PREFIX, "/fr/togo/");
  return RMO_JOB_REGEX.test(normalized);
}

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

    if (host.includes("jobrelais.com")) {
      return isJobrelaisJobUrl(path);
    }
    if (host.includes("emploitogo.info")) {
      return (
        path !== "/" &&
        !EMPLOITOGO_EXCLUSION_REGEX.test(path) &&
        EMPLOITOGO_JOB_REGEX.test(path)
      );
    }
    if (host.includes("emploitogo.com")) {
      return isEmploiTogoComJobUrl(path);
    }
    if (host.includes("anpetogo.org")) {
      return isAnpeJobUrl(path);
    }
    if (host.includes("emploi.tg")) {
      return isEmploiTgJobUrl(path);
    }
    if (host.includes("rmo-jobcenter.com")) {
      return isRmoJobUrl(path);
    }

    if (
      GENERIC_EXCLUSIONS_REGEX.test(path) ||
      GENERIC_BAD_PATHS_REGEX.test(path) ||
      path === "/" ||
      path === ""
    ) {
      return false;
    }

    return path.split("/").filter(Boolean).length >= 1;
  } catch {
    return false;
  }
}

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

// Helper: Generate Deterministic Canonical Fingerprint
function normalizeIdentityString(str) {
  if (!str) {
    return "";
  }
  return str
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]/g, "")
    .trim();
}

function generateFingerprint({ title, company, location }) {
  const normTitle = normalizeIdentityString(title);
  const normCompany = normalizeIdentityString(company);
  const normLocation = normalizeIdentityString(location || "togo");
  return `fp_${normTitle.slice(0, 32)}_${normCompany.slice(0, 32)}_${normLocation.slice(0, 16)}`;
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
const REMOVE_SELECTORS = [
  "script",
  "style",
  "noscript",
  "svg",
  "canvas",
  "iframe",
  "form",
  "button",
  "input",
  "textarea",
  "select",
  "nav",
  "header",
  "footer",
  "aside",
  ".header",
  "#header",
  ".footer",
  "#footer",
  ".sidebar",
  "#sidebar",
  ".menu",
  "#menu",
  ".nav",
  "#nav",
  ".navigation",
  ".widget",
  ".widgets",
  ".social",
  ".share",
  ".share-buttons",
  ".breadcrumb",
  ".breadcrumbs",
  ".pagination",
  ".related",
  ".similar-jobs",
  ".similar-posts",
  ".author-box",
  ".banner",
  ".cookie-notice",
  ".popup",
  ".modal",
  ".ad",
  ".ads",
  "[role='navigation']",
  "[role='banner']",
  "[role='contentinfo']",
  "[aria-label*='navigation' i]",
].join(", ");

const PRIMARY_SELECTORS = [
  ".job-description",
  ".job-details",
  ".job_description",
  ".entry-content",
  ".post-content",
  ".single-job-content",
  ".offer-content",
  ".offre-details",
  ".item-page",
  "article",
  "main",
  "[role='main']",
];

function extractUniversalJobContent(html) {
  if (!html) {
    return "";
  }

  const $ = load(html);
  $(REMOVE_SELECTORS).remove();

  let selectedHtml = "";
  for (const selector of PRIMARY_SELECTORS) {
    const el = $(selector);
    if (el.length > 0) {
      const textLen = el.text().trim().length;
      if (textLen >= 150) {
        selectedHtml = el.html() || "";
        break;
      }
    }
  }

  if (!selectedHtml) {
    selectedHtml = $("body").html() || "";
  }

  const markdown = turndown.turndown(selectedHtml);
  return cleanJobDescription(markdown);
}

// Helper: Fetch page text via Direct Readability Fetch or Jina Reader Fallback
async function fetchPageText(url) {
  console.log(`[Crawler] Fetching: ${url}`);

  // 1. Priorité au Direct Fetch local (rapide, sans bruit, 0 dépendance externe)
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
      const cleanMd = extractUniversalJobContent(html);
      if (cleanMd && cleanMd.length >= 150) {
        console.log(
          `[Crawler] ✅ Direct Universal Readability success (${cleanMd.length} chars)`
        );
        return cleanMd;
      }
    }
  } catch (err) {
    console.log(
      `[Crawler] Direct fetch failed for ${url}: ${err.message}. Trying Jina AI fallback.`
    );
  }

  // 2. Fallback Jina Reader (si bloqué ou JavaScript dynamique)
  try {
    const jinaHeaders = {
      "x-respond-with": "markdown",
      "x-remove-selector":
        "nav, header, footer, .header, #header, .footer, #footer, .menu, #menu, .navigation, .widget_recent_entries, .social, .share, .share-buttons, .breadcrumb, .breadcrumbs, .pagination, .related, .similar-jobs, .author-box, .banner, .cookie-notice, .popup, .modal, .ad, .ads",
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
        lower.includes("just a moment") ||
        lower.includes("enable javascript and cookies") ||
        lower.includes("requiring captcha") ||
        lower.includes("rate limited");

      if (!isError) {
        const cleanMd = cleanJobDescription(text);
        console.log(`[Crawler] ✅ Jina AI success (${cleanMd.length} chars)`);
        return cleanMd;
      }
    }
  } catch (err) {
    console.log(`[Crawler] Jina AI fetch failed for ${url}: ${err.message}.`);
  }

  return "";
}

const RE_MARKDOWN_IMAGES = /!\[[^\]]*\](?:\([^)]*\))?/g;
const RE_ADD_TO_ANY = /\[AddToAny\]\([^)]*\)/gi;
const RE_MORE_LINKS = /\[More…\]\([^)]*\)/gi;
const RE_TIME_COUNTERS = /(?:il y a \d+ (?:heures?|jours?|minutes?|mois))/gi;
const RE_NAV_PREV_NEXT = /(?:Précédent|Suivant)\s+RECRUTEMENT[^\n]*/gi;
const RE_COPYRIGHT = /©\s*\d{4}[^\n]*/gi;
const RE_EMAIL_CLOAK =
  /_?Cette adresse e-mail est protégée contre les robots spammeurs[^._\n]*[._]?/gi;
const RE_JS_VISUALIZE =
  /Vous devez activer le JavaScript pour la visualiser[._]*/gi;
const RE_CONSULT_SITES =
  /Cette annonce peut être consultée sur les sites[^\n]*/gi;
const RE_FOOTER_LINKS =
  /\*?\s*\[(?:Conditions d[’'']?Utilisation|Mentions l[ée]gales|Propri[ée]t[ée] Intellectuelle|Politique de confidentialit[ée]|Qui sommes-nous|FAQ|Contactez-nous)[^\]]*\]\([^)]*\)/gi;

const STOP_MARKERS = [
  /\n\s*(?:Offres?\s+d['’]emploi\s+similaires?|Articles?\s+similaires?|Autres?\s+offres?|Rejoignez\s+notre\s+cha[îi]ne|Partager\s+cette\s+offre|À\s+propos\s+de\s+l['’]auteur)/i,
  /\n\s*(?:Accueil\s*\n\s*A\s*Propos\s*\n\s*Contact)/i,
  /\n\s*La Totalité de nos opportunités/i,
];

const RE_JUNK_LINES = [
  /^(?:La Totalité de nos opportunités|Accueil|A Propos|Contact|Espace Candidats|Candidats - Inscription|Recherche avancée|Dernières opportunités|Opportunités au TOGO|COTONOU|Région\s*:|Date d'expiration|Catégories\s*:|Type d'offre|Lieu du travail\s*:)/i,
  /^(?:Emplois|Stages|Concours|Appel D'offres|Bourses D'études)\s*\(\d+\)/i,
  /^Recevez les nouvelles opportunités par email/i,
  /^Tous droits réservés/i,
  /^Publiée le \d{1,2}\.\d{1,2}\.\d{4}/i,
  /^Offre d'emploi Togo\s*:/i,
  /^\*?\s*\[(?:Conditions|Mentions|Propriété|Politique)/i,
  /^(?:Le Conseil International de la Croix-Rouge|Le Responsable du Développement)/i,
];

const START_MARKERS = [
  /\n\s*(?:Description\s*:|Missions\s*:|Dans le cadre|L['’]Agence nationale|Nous recrutons|Nous recherchons|Société de la place)/i,
  /^(?:Description\s*:|Missions\s*:|Dans le cadre|L['’]Agence nationale|Nous recrutons|Nous recherchons|Société de la place)/i,
];

function cleanJobDescription(rawDescription) {
  if (!rawDescription) {
    return "";
  }

  let text = rawDescription
    .replace(RE_MARKDOWN_IMAGES, "")
    .replace(RE_ADD_TO_ANY, "")
    .replace(RE_MORE_LINKS, "")
    .replace(RE_TIME_COUNTERS, "")
    .replace(RE_NAV_PREV_NEXT, "")
    .replace(RE_COPYRIGHT, "")
    .replace(RE_EMAIL_CLOAK, "")
    .replace(RE_JS_VISUALIZE, "")
    .replace(RE_CONSULT_SITES, "")
    .replace(RE_FOOTER_LINKS, "");

  for (const marker of STOP_MARKERS) {
    const match = text.match(marker);
    if (match && match.index !== undefined && match.index > 150) {
      text = text.slice(0, match.index).trim();
    }
  }

  for (const startMarker of START_MARKERS) {
    const match = text.match(startMarker);
    if (
      match &&
      match.index !== undefined &&
      match.index > 0 &&
      match.index < 800
    ) {
      text = text.slice(match.index).trim();
      break;
    }
  }

  const lines = text.split("\n");
  const filteredLines = [];

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) {
      filteredLines.push("");
      continue;
    }
    const isJunk = RE_JUNK_LINES.some((re) => re.test(trimmed));
    if (!isJunk) {
      filteredLines.push(line);
    }
  }

  let result = filteredLines.join("\n").trim();

  result = result
    // Cas 1 : Titres seuls sur une ligne (avec ou sans deux-points, avec ou sans dièses existants)
    .replace(
      /^(\s*)(?:#+\s*)?Missions?\s*(?:principales?|du poste)?\s*:?\s*$/gim,
      "\n\n## Missions principales\n\n"
    )
    .replace(
      /^(\s*)(?:#+\s*)?Profil\s*(?:recherch[ée]|du candidat|exig[ée])?\s*:?\s*$/gim,
      "\n\n## Profil recherché\n\n"
    )
    .replace(
      /^(\s*)(?:#+\s*)?Description\s*(?:du poste|de l['’]offre)?\s*:?\s*$/gim,
      "\n\n## Description du poste\n\n"
    )
    .replace(
      /^(\s*)(?:#+\s*)?Conditions?\s*(?:d['’]embauche|du contrat|de travail)?\s*:?\s*$/gim,
      "\n\n## Conditions de travail\n\n"
    )
    .replace(
      /^(\s*)(?:#+\s*)?(?:Comment postuler|Modalit[ée]s de candidature|Dossier de candidature|Pour postuler|Pi[èe]ces [àa] fournir)\s*:?\s*$/gim,
      "\n\n## Modalités de candidature\n\n"
    )
    .replace(
      /^(\s*)(?:#+\s*)?(?:Comp[ée]tences|Exigences|Qualifications|Responsabilit[ée]s|T[âa]ches)\s*(?:requises?|du poste)?\s*:?\s*$/gim,
      "\n\n## $2\n\n"
    )

    // Cas 2 : Titres collés au texte sur la même ligne (avec deux-points)
    .replace(
      /^(\s*)(?:#+\s*)?Conditions?\s*(?:d['’]embauche|du contrat|de travail)?\s*:\s*(.+)$/gim,
      "\n\n## Conditions de travail\n\n$2"
    )
    .replace(
      /^(\s*)(?:#+\s*)?(?:Comment postuler|Modalit[ée]s de candidature|Dossier de candidature|Pour postuler|Pi[èe]ces [àa] fournir)\s*:\s*(.+)$/gim,
      "\n\n## Modalités de candidature\n\n$2"
    )
    .replace(
      /^(\s*)(?:#+\s*)?Description\s*(?:du poste|de l['’]offre)?\s*:\s*(.+)$/gim,
      "\n\n## Description du poste\n\n$2"
    )
    .replace(
      /^(\s*)(?:#+\s*)?Missions?\s*(?:principales?|du poste)?\s*:\s*(.+)$/gim,
      "\n\n## Missions principales\n\n$2"
    )
    .replace(
      /^(\s*)(?:#+\s*)?Profil\s*(?:recherch[ée]|du candidat|exig[ée])?\s*:\s*(.+)$/gim,
      "\n\n## Profil recherché\n\n$2"
    )

    // Cas 3 : Sans deux-points mais avec majuscule après (ex: "Conditions de travail Poste de...")
    .replace(
      /^(\s*)Conditions?\s*(?:d['’]embauche|du contrat|de travail)\s+([A-ZÀ-Ÿ].+)$/gim,
      "\n\n## Conditions de travail\n\n$2"
    )
    .replace(
      /^(\s*)Modalit[ée]s de candidature\s+([A-ZÀ-Ÿ].+)$/gim,
      "\n\n## Modalités de candidature\n\n$2"
    )
    .replace(
      /^(\s*)Missions?\s*(?:principales?|du poste)\s+([A-ZÀ-Ÿ].+)$/gim,
      "\n\n## Missions principales\n\n$2"
    )
    .replace(
      /^(\s*)Profil\s*(?:recherch[ée]|du candidat|exig[ée])\s+([A-ZÀ-Ÿ].+)$/gim,
      "\n\n## Profil recherché\n\n$2"
    );

  return result.replace(/\n{3,}/g, "\n\n").trim();
}

// Helper: Extract Clean Full Markdown announcement
function extractCleanFullDescription(rawPageText, aiDescription) {
  if (aiDescription && aiDescription.length > 80) {
    return cleanJobDescription(aiDescription);
  }
  if (!rawPageText) {
    return aiDescription || "";
  }
  return cleanJobDescription(rawPageText);
}

// Helper: Extract URLs matching job patterns from HTML or text/markdown content
function extractJobUrls(content, baseUrl) {
  const found = new Set();
  const baseHostname = new URL(baseUrl).hostname.replace(WWW_PREFIX_REGEX, "");

  // Prise en charge de la balise <base href="..."> si présente (ex: RMO Jobcenter)
  let effectiveBaseUrl = baseUrl;
  const baseMatch = content.match(RE_BASE_TAG);
  if (baseMatch) {
    try {
      effectiveBaseUrl = new URL(baseMatch[1], baseUrl).href;
    } catch {
      // Ignore et conserve baseUrl par défaut
    }
  }

  function processCandidateUrl(rawUrl) {
    if (!rawUrl) {
      return;
    }
    try {
      const parsed = new URL(rawUrl, effectiveBaseUrl);
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

  // 2. Regex parsing for Markdown links: [title](url) - captures both relative and absolute links
  const mdRegex = /\[([^\]]*?)\]\(([^\s)]+)\)/g;
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

// Helper: Fetch source listing catalog and extract job candidate URLs from raw HTML (with Jina fallback)
async function fetchSourceJobUrls(sourceUrl) {
  const found = new Set();
  const baseHostname = new URL(sourceUrl).hostname.replace(
    WWW_PREFIX_REGEX,
    ""
  );

  console.log(`[Crawler] Fetching listing: ${sourceUrl}`);

  // 1. Direct HTML fetch (preserves entire DOM tree without premature Markdown reduction)
  let rawHtml = "";
  try {
    const directRes = await fetch(sourceUrl, {
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
      rawHtml = await directRes.text();
    }
  } catch (err) {
    console.log(
      `[Crawler] Direct listing fetch failed for ${sourceUrl}: ${err.message}. Trying Jina fallback.`
    );
  }

  const lowerHtml = rawHtml.toLowerCase();
  const isBlocked =
    rawHtml.length < 200 ||
    lowerHtml.includes("403 forbidden") ||
    lowerHtml.includes("blocked by cloudflare") ||
    lowerHtml.includes("just a moment") ||
    lowerHtml.includes("enable javascript and cookies") ||
    lowerHtml.includes("requiring captcha");

  if (rawHtml && !isBlocked) {
    const $ = load(rawHtml);

    // Detect <base href="..."> if present in HTML head (e.g. RMO Jobcenter)
    let effectiveBaseUrl = sourceUrl;
    const baseHref = $("base[href]").attr("href");
    if (baseHref) {
      try {
        effectiveBaseUrl = new URL(baseHref, sourceUrl).href;
      } catch {
        // Keep sourceUrl by default
      }
    }

    $("a[href]").each((_, el) => {
      const rawUrl = $(el).attr("href");
      if (!rawUrl) {
        return;
      }
      try {
        const parsed = new URL(rawUrl, effectiveBaseUrl);
        parsed.hash = "";
        const cleaned = parsed.href;
        const candidateHostname = parsed.hostname.replace(WWW_PREFIX_REGEX, "");

        if (candidateHostname !== baseHostname) {
          return;
        }

        if (isStrictJobUrl(cleaned, sourceUrl)) {
          found.add(cleaned);
        }
      } catch {
        // Ignore invalid URL
      }
    });

    if (found.size > 0) {
      console.log(
        `[Crawler] ✅ Direct HTML listing extraction success (${found.size} URLs found)`
      );
      return Array.from(found).slice(0, CONFIG.maxUrlsPerSource);
    }
  }

  // 2. Fallback to Jina AI Reader if direct HTML had 0 URLs or failed
  console.log(
    `[Crawler] Falling back to Jina Reader for listing ${sourceUrl}...`
  );
  try {
    const jinaHeaders = {
      "x-respond-with": "markdown",
      "User-Agent": "Mozilla/5.0 (compatible; SolyBot/2.0; +https://soly.work)",
    };
    if (CONFIG.jinaApiKey) {
      jinaHeaders.Authorization = `Bearer ${CONFIG.jinaApiKey}`;
    }

    const jinaRes = await fetch(`https://r.jina.ai/${sourceUrl}`, {
      headers: jinaHeaders,
      signal: AbortSignal.timeout(25_000),
    });

    if (jinaRes.ok) {
      const markdown = await jinaRes.text();
      const urlsFromMd = extractJobUrls(markdown, sourceUrl);
      for (const u of urlsFromMd) {
        found.add(u);
      }
    }
  } catch (err) {
    console.log(
      `[Crawler] Jina listing fallback failed for ${sourceUrl}: ${err.message}`
    );
  }

  return Array.from(found).slice(0, CONFIG.maxUrlsPerSource);
}

// Single call to Google Gemini endpoint
async function callGeminiApi(apiKey, model, pageText, sourceUrl) {
  if (!apiKey) {
    return null;
  }

  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;

  const prompt = `Tu es un extracteur RH de précision spécialisé dans la structuration d'offres d'emploi.
Extrais STRICTEMENT l'offre d'emploi sous forme d'un objet JSON valide avec cette structure (sans backticks markdown \`\`\`json) :
{
  "title": "Titre exact du poste",
  "company": "Nom réel de l'entreprise ou 'Entreprise confidentielle'",
  "location": "Lomé, Togo (ou Togo / Remote)",
  "description": "Corps complet et fidèle de l'annonce structuré en Markdown pur (avec titres ## Missions, ## Profil recherché, ## Conditions de travail, ## Modalités de candidature). Exclus strictement toute image, pub, navigation ou footer.",
  "summary": "Résumé factuel de 2-3 phrases en français",
  "contractType": "CDI" | "CDD" | "Stage" | "Freelance" | "Alternance" | "Autre",
  "workMode": "office" | "hybrid" | "remote",
  "sector": "Tech" | "Finance" | "Commercial" | "RH" | "Santé" | "Marketing" | "Logistique" | "Éducation" | "BTP" | "Juridique" | "Agriculture" | "Hôtellerie" | "ONG" | "Design" | "Autre",
  "salary": "Salaire explicite uniquement ou null (NE JAMAIS INVENTER OU ESTIMER DE SALAIRE)",
  "skills": ["Compétence 1", "Compétence 2"],
  "applyUrl": "URL de candidature, email ou téléphone",
  "howToApply": "Instructions précises de candidature"
}

RÈGLES DE FIDÉLITÉ STRICTE :
- ZÉRO INVENTION DE SALAIRE : Si aucun montant n'est écrit dans le texte, mets STRICTEMENT salary: null.
- ZÉRO COMMENTAIRE IA : Aucun ajout personnel ou introduction artificielle.

Texte source (URL: ${sourceUrl}) :
${pageText.slice(0, 15_000)}`;

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
      signal: AbortSignal.timeout(CONFIG.geminiTimeoutMs || 35_000),
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

  const prompt = `Tu es un expert RH de haut niveau. Extrais l'offre d'emploi sous forme d'un objet JSON strict avec :
- title: Titre exact du poste
- company: Nom réel de l'entreprise ou 'Entreprise confidentielle'
- location: Ville, Togo (ou Remote)
- description: Rédige le corps COMPLET, EXHAUSTIF et PROFESSIONNEL de l'annonce en Markdown élégant (avec des sections Markdown ## Missions, ## Profil recherché, ## Conditions de travail, ## Modalités de candidature). Exclus STRICTEMENT tout lien de navigation, fil d'Ariane, vue(s), offres récentes, articles similaires ou publicité.
- summary: Résumé percutant de 2-3 phrases en français
- contractType: 'CDI' | 'CDD' | 'Stage' | 'Freelance' | 'Alternance' | 'Autre'
- workMode: 'office' | 'hybrid' | 'remote'
- sector: Secteur d'activité
- skills: Liste de compétences requises
- salary: Salaire ou null
- applyUrl: Email, URL ou téléphone pour postuler
- howToApply: Consignes précises pour postuler

Texte (URL: ${sourceUrl}) :
${pageText.slice(0, 12_000)}`;

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

// Sequential AI Extraction Cascade (Gemini Primary/Fallback -> Groq Primary -> Groq Fallback)
async function extractJobWithAiCascade(detailText, jobUrl) {
  console.log("[AI] Parsing job with Gemini (Primary / Fallback)...");
  let parsed = await parseWithGemini(detailText, jobUrl);

  if (!(parsed?.title && parsed?.company) && CONFIG.groqModel) {
    console.log(
      `[AI] ⚠️ Gemini failed or returned incomplete. Falling back to Groq (${CONFIG.groqModel})...`
    );
    parsed = await parseWithGroq(detailText, jobUrl, CONFIG.groqModel);
  }

  if (!(parsed?.title && parsed?.company) && CONFIG.groqFallbackModel) {
    console.log(
      `[AI] ⚠️ Groq primary failed. Falling back to Groq backup (${CONFIG.groqFallbackModel})...`
    );
    parsed = await parseWithGroq(detailText, jobUrl, CONFIG.groqFallbackModel);
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

  const payload = {
    title: parsed.title,
    company: parsed.company,
    location: parsed.location || "Togo",
    description:
      parsed.description ||
      extractCleanFullDescription(detailText, parsed.description),
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

// Process a batch of candidate URLs for a single source
async function processSourceBatch(source, errors) {
  console.log(`\n--- Scanning Source: ${source.name} ---`);
  const jobUrls = await fetchSourceJobUrls(source.url);
  console.log(
    `[Scan] Found ${jobUrls.length} potential job URLs for ${source.name}.`
  );

  let newCount = 0;
  let dupCount = 0;
  let extractedCount = 0;

  for (const jobUrl of jobUrls) {
    try {
      const result = await processJobCandidate(jobUrl);
      if (result) {
        extractedCount++;
        if (result.ingestRes?.isNew) {
          newCount++;
          console.log(
            `[Ingest] ✨ New job created & queued: "${result.payload.title}" (ID: ${result.ingestRes.jobId})`
          );
        } else {
          dupCount++;
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

  return {
    discovered: jobUrls.length,
    extracted: extractedCount,
    newCount,
    dupCount,
  };
}

function printSourceBreakdown(sourceStats) {
  console.log("--------------------------------------------------");
  console.log("📋 Per-Source Breakdown:");
  for (const s of sourceStats) {
    let statusIcon = "🔁";
    if (s.discovered === 0) {
      statusIcon = "⚠️";
    } else if (s.new > 0) {
      statusIcon = "✨";
    }
    console.log(
      `  ${statusIcon} ${s.name.padEnd(24)}: ${s.discovered} found | ${s.new} new | ${s.duplicates} dup`
    );
  }
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

  const activeSources = await loadTargetSources();
  const sourceStats = [];

  for (const source of activeSources) {
    const stats = await processSourceBatch(source, errors);
    totalDiscoveredUrls += stats.discovered;
    totalExtracted += stats.extracted;
    totalIngestedNew += stats.newCount;
    totalDuplicates += stats.dupCount;

    sourceStats.push({
      name: source.name,
      discovered: stats.discovered,
      new: stats.newCount,
      duplicates: stats.dupCount,
    });
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
  printSourceBreakdown(sourceStats);
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
