## LinkedIn Posts Automation (Free Stack)

This project automates searching LinkedIn posts for "Data Analyst hiring", filters for roles suitable for 0–2 years experience using a local LLM (Ollama), extracts details, and writes results to an Excel file.

### Recommended: Node.js stack (this environment)
- Playwright (browser automation)
- Ollama (local LLM runtime; e.g., `llama3`, `mistral`)
- xlsx (Excel I/O)

#### Setup
```bash
# Node.js dependencies
cd /workspace/node-linkedin-scraper
npm install

# Playwright browsers (Chromium)
npx playwright install chromium

# Ollama (local LLM)
curl -fsSL https://ollama.com/install.sh | sh
ollama pull llama3  # or mistral
```

#### First-time login (one-time)
```bash
# Option A: Headless with credentials (may hit 2FA/CAPTCHA)
export LINKEDIN_EMAIL="you@example.com"
export LINKEDIN_PASSWORD="your_password"
node src/index.js --login --headless

# Option B: Visible browser for manual/2FA
node src/index.js --login --headless=false
```
This creates `storage_state.json` for reuse.

#### Run the scraper
```bash
node src/index.js \
  --query "Data Analyst hiring" \
  --max-posts 60 \
  --output results.xlsx \
  --ollama-model llama3 \
  --headless=true
```
Env overrides: `LINKEDIN_EMAIL`, `LINKEDIN_PASSWORD`, `OLLAMA_HOST`, `OLLAMA_MODEL`.

### Python alternative (may require system packages on Python 3.13)
A Python version exists at `/workspace/linkedin_scraper.py`, but on Python 3.13 this environment blocks venv creation and certain native deps. If you still want Python, install system packages and use `--break-system-packages` cautiously.

### Output
Excel columns:
- timestamp, post_url, poster_name, poster_profile_url, poster_linkedin_id
- role_title, min_years_experience, max_years_experience, skills
- location, job_type, contact, post_excerpt

### Notes
- Respect LinkedIn’s Terms of Service and rate limits.
- Use your own account; prefer `--headless=false` for debugging/login.
- Script includes a fallback heuristic if LLM output is invalid.
