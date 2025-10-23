//
// Projekt Prometheus: Kombinierter Batch-Job (Sentinel + Forge)
// Vercel Serverless Function (Hobby-Plan kompatibel)
//
import { createClient, SupabaseClient } from "@supabase/supabase-js";
import { GoogleGenerativeAI } from "@google/generative-ai";
import { default as Parser } from "rss-parser";
import axios from "axios";
import * as cheerio from "cheerio";
// @ts-ignore
import pdf from "pdf-parse/lib/pdf-parse.js";
import type { VercelRequest, VercelResponse } from '@vercel/node';

// --- Konfiguration ---
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_KEY;
const geminiApiKey = process.env.GEMINI_API_KEY;

// Vercel Hobby Plan Timeout (max 5 Minuten)
// Wir setzen ein Sicherheitslimit von 4.5 Minuten (270.000 ms)
const MAX_EXECUTION_TIME_MS = 270000;

const RSS_FEED_URLS = [
  "https://www.europarl.europa.eu/rss/committee/envi/en.xml",
  "https://www.europarl.europa.eu/rss/committee/itre/en.xml",
  "https://www.europarl.europa.eu/rss/committee/imco/en.xml",
];

// --- Globale Instanzen ---
let supabase: SupabaseClient;
let scoutModel: any; // GenerativeModel
let generalModel: any; // GenerativeModel

// --- Vercel Handler ---
export default async function handler(
  req: VercelRequest,
  res: VercelResponse
) {
  if (!supabaseUrl || !supabaseServiceKey || !geminiApiKey) {
    return res.status(500).json({ error: "Env-Variablen nicht gesetzt." });
  }
  
  supabase = createClient(supabaseUrl, supabaseServiceKey);
  const genAI = new GoogleGenerativeAI(geminiApiKey);
  scoutModel = genAI.getGenerativeModel({ model: "gemini-2.5-flash" });
  generalModel = genAI.getGenerativeModel({ model: "gemini-2.5-pro" });

  const startTime = Date.now();
  
  // --- SCHRITT 1: SENTINEL AUSFÜHREN ---
  console.log("[Batch] Starte Sentinel-Phase...");
  const sentinelResult = await runSentinel();
  console.log(`[Batch] Sentinel-Phase beendet. ${sentinelResult.newEntries} neue Einträge.`);
  
  // --- SCHRITT 2: FORGE AUSFÜHREN ---
  console.log("[Batch] Starte Forge-Phase...");
  const forgeResult = await runForge(startTime);
  console.log(`[Batch] Forge-Phase beendet.`);

  const executionTime = Date.now() - startTime;
  
  res.status(200).json({ 
    message: "Batch-Lauf (Sentinel + Forge) abgeschlossen.",
    executionTimeMs: executionTime,
    sentinel: sentinelResult,
    forge: forgeResult
  });
}

// ===========================================
// SENTINEL LOGIK (Phase 1)
// ===========================================
async function runSentinel() {
  console.log(`[Sentinel] Überwache ${RSS_FEED_URLS.length} Feeds.`);
  let newEntriesCount = 0;
  
  const parser = new Parser();
  
  for (const feedUrl of RSS_FEED_URLS) {
    try {
      const response = await axios.get(feedUrl, {
        headers: { "User-Agent": "Mozilla/5.0" },
        timeout: 10000,
      });
      const feed = await parser.parseString(response.data);
      console.log(`[Sentinel] Feed gefunden: "${feed.title}"`);
      
      const itemsToProcess = (feed.items || []).slice(0, 15).reverse();

      for (const item of itemsToProcess) {
        const { title, link, pubDate } = item;
        if (!link || !title) continue;

        const { data: existing, error: checkError } = await supabase
          .from("legislations")
          .select("link")
          .eq("link", link)
          .maybeSingle();

        if (existing || checkError) continue;

        const { error: insertError }_ = await supabase
          .from("legislations")
          .insert({
            title: title,
            link: link,
            publish_date: pubDate ? new Date(pubDate) : new Date(),
            pdf_storage_path: null,
            status: "pending",
          });

        if (!insertError) {
          console.log(`[Sentinel] NEU: ${title}`);
          newEntriesCount++;
        }
      }
    } catch (error: any) {
      console.error(`[Sentinel] FEHLER bei Feed ${feedUrl}:`, error.message);
    }
  }
  return { newEntries: newEntriesCount };
}

// ===========================================
// FORGE LOGIK (Phase 2)
// ===========================================
async function runForge(startTime: number) {
  let jobsProcessed = 0;
  let jobsIgnored = 0;
  let jobsFailed = 0;

  while (Date.now() - startTime < MAX_EXECUTION_TIME_MS) {
    let item;
    try {
      const { data: foundItem, error: selectError } = await supabase
        .from("legislations")
        .select("*")
        .eq("status", "pending")
        .limit(1)
        .single();

      if (selectError || !foundItem) {
        console.log("[Forge] Warteschlange ist leer.");
        break; 
      }
      item = foundItem;

      console.log(`[Forge] VERARBEITE: ${item.title}`);

      await supabase
        .from("legislations")
        .update({ status: "processing" })
        .eq("id", item.id);

      const pdfUrl = await scrapeAndFindPdfUrl(item.link);
      const text = await downloadAndParsePdf(pdfUrl);

      await supabase
        .from("legislations")
        .update({ pdf_storage_path: pdfUrl })
        .eq("id", item.id);

      const scoutReports = await runScoutAnalysis(text);
      const finalJsonString = await runGeneralAnalysis(scoutReports, item.title);
      const analysisJson = JSON.parse(finalJsonString);

      if (!analysisJson.confidence_score_percent || analysisJson.confidence_score_percent <= 0) {
        console.log(`[Forge] ERFOLG (Ignoriert): Score (0) zu niedrig. ${item.title}`);
        await supabase
          .from("legislations")
          .update({ status: "completed" })
          .eq("id", item.id);
        jobsIgnored++;
        continue;
      }

      await supabase.from("analysis_results").insert({
        legislation_id: item.id,
        analysis_json: analysisJson,
        confidence_score: analysisJson.confidence_score_percent || 0,
        time_horizon_months: analysisJson.time_horizon_months || 0,
      });

      await supabase
        .from("legislations")
        .update({ status: "completed" })
        .eq("id", item.id);

      console.log(`[Forge] ERFOLG: ${item.title}`);
      jobsProcessed++;

    } catch (error: any) {
      console.error(`[Forge] PIPELINE-FEHLER bei ${item ? item.title : "Unbekanntem Job"}:`, error.message);
      jobsFailed++;
      if (item) {
        await supabase
          .from("legislations")
          .update({ status: "failed", pdf_storage_path: error.message })
          .eq("id", item.id);
      }
    }
  }

  const executionTime = Date.now() - startTime;
  console.log(`[Forge] Batch-Lauf beendet nach ${executionTime}ms.`);
  
  return {
    processed: jobsProcessed,
    ignored: jobsIgnored,
    failed: jobsFailed,
    executionTimeMs: executionTime
  };
}


// --- Alle Hilfsfunktionen (Scraper, Parser, Scouts, General, Backoff, etc.) ---

async function scrapeAndFindPdfUrl(pageUrl: string): Promise<string> {
  console.log(`[Forge] Scrape... ${pageUrl}`);
  const { data: html } = await axios.get(pageUrl, {
    headers: { "User-Agent": "Mozilla/5.0" },
    timeout: 15000,
  });
  const $ = cheerio.load(html);
  let pdfLink: string | undefined;
  $('a[href$=".pdf"][href*="EN"]').each((i, el) => {
    const href = $(el).attr("href");
    if (href) { pdfLink = href; return false; }
  });
  if (pdfLink) {
    return pdfLink.startsWith("/") ? `https://www.europarl.europa.eu${pdfLink}` : pdfLink;
  }
  throw new Error(`Konnte keinen _EN.pdf Link auf der Seite finden: ${pageUrl}`);
}

async function downloadAndParsePdf(pdfUrl: string): Promise<string> {
  console.log(`[Forge] Lade PDF... ${pdfUrl}`);
  const response = await axios.get(pdfUrl, {
    responseType: "arraybuffer",
    timeout: 15000,
  });
  const buffer = response.data;
  const data = await pdf(buffer);
  console.log(`[Forge] PDF geparst: ${data.text.length} Zeichen.`);
  return data.text;
}

async function runScoutAnalysis(fullText: string): Promise<string[]> {
  console.log("[Forge] Starte Scout-Analyse (Map) mit Gemini 2.5 Flash...");
  const chunks = fullText.match(/[\s\S]{1,10000}/g) || [];
  console.log(`[Forge] Text in ${chunks.length} Blöcke aufgeteilt.`);
  
  const scoutPromises = chunks.map((chunk, i) => {
    const scoutPrompt = `
You are a forensic regulatory analyst at a quantitative hedge fund. Analyze ONLY this text segment of an EU regulation.
**FOCUS AREAS (in priority order):**
1. **Compliance Cost Differentials**: Which companies/sectors face asymmetric compliance burdens?
2. **Timing Arbitrage**: Phase-in periods, grandfather clauses, implementation delays
3. **Technical Standards**: Specific metrics/thresholds that create winners/losers
4. **Substitution Effects**: Forced technology/materials shifts
5. **Reporting Requirements**: Data/transparency burdens that create competitive advantages
**SECTORS TO ANALYZE:**
- Semiconductors: Equipment bans, export controls, IP transfer rules
- Energy: Carbon costs, grid access, capacity mechanisms
- Chemicals: REACH amendments, production process restrictions
- Logistics: Cross-border procedures, documentation burdens
- Automotive: Technical standards, testing requirements
- Banking: Capital requirements, reporting burdens, product restrictions
**OUTPUT FORMAT:**
If you find material impacts, provide:
- Specific article/reference numbers
- Quantitative thresholds mentioned (€ amounts, percentage requirements, timing)
- Clear identification of relative winners/losers
- Compliance timeline with key dates
If no material impacts found: "NO ALPHA - IGNORE"
**TEXT SEGMENT:**
"""
${chunk}
"""
`;
    return exponentialBackoff(() =>
      scoutModel.generateContent(scoutPrompt).then((result) => {
        console.log(`[Forge] Scout ${i + 1}/${chunks.length} fertig.`);
        return result.response.text();
      })
    );
  });
  const scoutReports = await Promise.all(scoutPromises);
  return scoutReports.filter((report) => !report.includes("NO ALPHA - IGNORE"));
}

async function runGeneralAnalysis(
  scoutReports: string[],
  lawTitle: string
): Promise<string> {
  console.log("[Forge] Starte General-Analyse (Reduce) mit Gemini 2.5 Pro...");
  if (scoutReports.length === 0) {
    console.log("[Forge] Keine relevanten Berichte von Scouts. General wird übersprungen.");
    return JSON.stringify({
      law_title: lawTitle,
      summary_of_law: "Keine relevanten finanziellen Auswirkungen von Scouts identifiziert.",
      the_hidden_opportunity: "N/A",
      affected_sectors: [],
      specific_companies_short: [],
      specific_companies_long: [],
      confidence_score_percent: 0,
      time_horizon_months: 0,
      trade_strategy: { primary_trade: "Keine Aktion.", hedge_components: [], position_sizing: "N/A", catalyst_timing: "N/A", risk_factors: [] },
      quantitative_metrics: { estimated_impact_bps: 0, liquidity_requirement: "N/A", correlation_breakdown: "N/A", capacity_estimate_millions: 0 }
    });
  }

  const aggregatedReports = scoutReports.join("\n---\n");
  const prompt = `
You are the Chief Investment Officer of a multi-billion dollar quantitative hedge fund. Your analysts have provided raw intelligence on a new EU regulation. Your task is to synthesize this into an executable investment thesis.
**CRITICAL THINKING FRAMEWORK:**
1. **Second-Order Effects**: Look beyond immediate compliance costs to secondary market impacts
2. **Regulatory Arbitrage**: Identify loopholes, timing advantages, and jurisdictional gaps
3. **Supply Chain Ripple Effects**: Map upstream/downstream winners and losers
4. **Capital Reallocation**: Where will capital flee and where will it flow?
5. **Asymmetric Opportunities**: Small regulatory change → large market impact
**TITEL DES GESETZES: ${lawTitle}**
**ANALYST RAW INTELLIGENCE:**
"""
${aggregatedReports}
"""
**REQUIRED OUTPUT FORMAT (STRICT JSON):**
{
  "law_title": "${lawTitle}",
  "summary_of_law": "Concise 2-3 sentence explanation of the regulation's core mechanism",
  "the_hidden_opportunity": "Specific, non-obvious alpha source with clear causal chain",
  "affected_sectors": ["sector1", "sector2"],
  "specific_companies_short": [{"company": "Ticker/Name", "rationale": "Specific regulatory exposure", "timeframe_months": 6, "conviction_score": 0.8}],
  "specific_companies_long": [{"company": "Ticker/Name", "rationale": "Regulatory beneficiary with moat", "timeframe_months": 12, "conviction_score": 0.9}],
  "confidence_score_percent": 85,
  "time_horizon_months": 18,
  "trade_strategy": {
    "primary_trade": "Specific instrument/sector pairs trade",
    "hedge_components": ["Protective puts on X", "Calendar spreads on Y"],
    "position_sizing": "Concentrated vs diversified approach",
    "catalyst_timing": "Key legislative/implementation dates",
    "risk_factors": ["Regulatory reversal risk", "Timing slippage"]
  },
  "quantitative_metrics": {
    "estimated_impact_bps": 250,
    "liquidity_requirement": "High/Medium/Low",
    "correlation_breakdown": "Will this trade correlate with broader market?",
    "capacity_estimate_millions": 50
  }
}
**ANALYSIS PRINCIPLES:**
- Be ruthless and objective - sentiment doesn't move markets
- Focus on implementation timing and enforcement reality
- Identify the ONE trade that delivers 80% of the alpha
- Consider counterparty positioning and crowded trades
- Always include the hedge and risk management
Your analysis must be immediately actionable by our trading desk.
`;
  const result = await exponentialBackoff(() => generalModel.generateContent(prompt));
  let jsonText = result.response.text().replace(/^```json\n/, "").replace(/\n```$/, "");
  try {
    JSON.parse(jsonText);
  } catch (e) {
    console.error("[Forge] General hat kein valides JSON zurückgegeben. Versuche Reparatur...");
    jsonText = await fixBrokenJson(jsonText, lawTitle);
  }
  console.log("[Forge] General-Analyse fertig.");
  return jsonText;
}

async function exponentialBackoff<T>(fn: () => Promise<T>, retries = 5, delay = 1000): Promise<T> {
  try {
    return await fn();
  } catch (error: any) {
    if (retries > 0) {
      console.warn(`[Forge] API-Fehler (Rate Limit?). Versuche erneut in ${delay}ms...`);
      await new Promise((res) => setTimeout(res, delay));
      return exponentialBackoff(fn, retries - 1, delay * 2);
    }
    throw error;
  }
}

async function fixBrokenJson(brokenJson: string, title: string): Promise<string> {
  const fixPrompt = `
    Der folgende Text sollte valides JSON sein, ist es aber nicht. 
    Bitte korrigiere es und gib NUR das valide JSON zurück.
    Der Titel des Gesetzes lautet: "${title}"
    DEFEKTES JSON:
    ${brokenJson}
  `;
  const result = await generalModel.generateContent(fixPrompt);
  return result.response.text().replace(/^```json\n/, "").replace(/\n```$/, "");
}
