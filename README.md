# Soly Autonomous Job Crawler 🚀

Moteur de scraping et d'ingestion d'offres d'emploi autonome pour [Soly](https://soly.work).

## 📌 Fonctionnalités

- **Extraction ciblée par source** : Jobrelais Togo, Emploi Togo, ANPE Togo.
- **Filtres anti-faux-jobs** :
  - Rejet déterministe des URLs non-annonces.
  - Filtre structurel pré-IA (vérification des sections obligatoires *missions, profil, conditions*).
  - Filtre sémantique post-IA.
- **Cascade IA Multi-Modèles** :
  - Google Gemini (`gemini-flash-latest` / `gemini-flash-lite-latest`) avec double clé (Principale + Fallback).
  - Groq (`llama-3.3-70b-versatile` / `llama-3.1-8b-instant`).
- **Publication automatique** : Ingestion atomique vers l'API Soly et diffusion sur les canaux Telegram.

---

## ⚙️ Configuration des Secrets GitHub Actions

Dans **Settings** → **Secrets and variables** → **Actions** :

| Secret | Description |
| :--- | :--- |
| `APP_URL` | URL de votre instance Soly (ex: `https://soly.work`) |
| `SCRAPER_SECRET` | Clé secrète d'ingestion Soly (`SYNC_KEY` ou `INGESTION_KEY`) |
| `GEMINI_API_KEY` | Clé Google Gemini principale (Pro) |
| `GEMINI_API_KEY_FALLBACK` | Clé Google Gemini secondaire (Gratuite) |
| `GROQ_API_KEY` | Clé API Groq |
| `TELEGRAM_BOT_TOKEN` | Token du bot Telegram |
| `TELEGRAM_ADMIN_CHAT_ID` | Identifiant du canal ou groupe Telegram (ex: `@ShareitJobTG`) |

---

## 💻 Exécution en Local

```bash
# Tester sans enregistrer (mode dry-run)
node scrape.mjs --dry-run

# Exécuter l'ingestion réelle
node scrape.mjs
```
