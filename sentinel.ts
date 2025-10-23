//
// Projekt Prometheus: "Der Sentinel" (Phase 1)
// Vercel Serverless Function
//
import { createClient, SupabaseClient } from "@supabase/supabase-js";
import { default as Parser } from "rss-parser";
import axios from "axios";
import type { VercelRequest, VercelResponse } from '@vercel/node';

// --- Konfiguration ---
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_KEY;

const RSS_FEED_URLS = [
  "https://www.europarl.europa.eu/rss/committee/envi/en.xml",
  "https://www.europarl.europa.eu/rss/committee/itre/en.xml",
  "https://www.europarl.europa.eu/rss/committee/imco/en.xml",
];

// --- Vercel Handler ---
export default async function handler(
  req: VercelRequest,
  res: VercelResponse
) {
  if (!supabaseUrl || !supabaseServiceKey) {
    return res.status(500).json({ error: "Supabase-Variablen nicht gesetzt." });
  }
  const supabase: SupabaseClient = createClient(supabaseUrl, supabaseServiceKey);
  
  console.log(`[Sentinel] Cron Job gestartet. Überwache ${RSS_FEED_URLS.length} Feeds.`);

  const parser = new Parser();
  let newEntriesCount = 0;
  
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

        // 1. DUPLIKAT-PRÜFUNG
        const { data: existing, error: checkError }_ = await supabase
          .from("legislations")
          .select("link")
          .eq("link", link)
          .maybeSingle();

        if (existing || checkError) {
          continue; // Bereits erfasst oder Fehler
        }

        // 2. NEUEN EINTRAG ERSTELLEN
        const { error: insertError } = await supabase
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

  console.log(`[Sentinel] Lauf beendet. ${newEntriesCount} neue Einträge hinzugefügt.`);
  res.status(200).json({ message: `Sentinel run complete. ${newEntriesCount} new entries added.` });
}
