import { neon } from "@netlify/neon";

const sql = neon();

// ─── CORS Headers ────────────────────────────────────────────────────────
const headers = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,PATCH,DELETE,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const json = (data, status = 200) => new Response(JSON.stringify(data), { status, headers });
const err = (msg, status = 500) => json({ error: msg }, status);

// ─── DB Setup (runs once on first call) ──────────────────────────────────
async function ensureTables() {
  try {
    await sql`SELECT 1 FROM users LIMIT 1`;
    // Migrate: update role constraint and upsert real users
    await sql`ALTER TABLE users DROP CONSTRAINT IF EXISTS users_role_check`;
    await sql`ALTER TABLE users ADD CONSTRAINT users_role_check CHECK (role IN ('setter', 'admin', 'opener'))`;
    await sql`
      INSERT INTO users (id, name, email, role, close_id) VALUES
        ('u1', 'Maxim Nickel', 'maxim@advisy.de', 'setter', 'usr_maxim'),
        ('u2', 'Sergej Janle', 'sergej@advisy.de', 'setter', 'usr_sergej'),
        ('u3', 'Tim Vogel', 'tim@advisy.de', 'admin', 'usr_tim'),
        ('u4', 'Pana', 'pana@advisy.de', 'opener', null),
        ('u5', 'Jaky', 'jaky@advisy.de', 'opener', null),
        ('u6', 'Sabine', 'sabine@advisy.de', 'opener', null)
      ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, role = EXCLUDED.role`;
  } catch (e) {
    if (e.message && e.message.includes("does not exist")) {
    console.log("Creating tables...");
    await sql`
      CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        role TEXT NOT NULL CHECK (role IN ('setter', 'admin', 'opener')),
        close_id TEXT,
        active BOOLEAN DEFAULT true,
        created_at TIMESTAMPTZ DEFAULT now()
      )`;
    await sql`
      CREATE TABLE IF NOT EXISTS appointments (
        id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        close_id TEXT UNIQUE,
        opener_name TEXT,
        setter_id TEXT REFERENCES users(id),
        datetime TIMESTAMPTZ NOT NULL,
        imported_status TEXT DEFAULT 'scheduled',
        final_status TEXT DEFAULT 'open' CHECK (final_status IN ('open','show','no_show','rescheduled','cancelled')),
        source TEXT DEFAULT 'close',
        lead_name TEXT,
        company TEXT,
        close_lead_id TEXT,
        notes TEXT,
        imported_at TIMESTAMPTZ DEFAULT now(),
        status_at TIMESTAMPTZ,
        status_by TEXT REFERENCES users(id),
        created_at TIMESTAMPTZ DEFAULT now(),
        updated_at TIMESTAMPTZ DEFAULT now()
      )`;
    await sql`
      CREATE TABLE IF NOT EXISTS activities (
        id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        msg TEXT NOT NULL,
        user_id TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
      )`;
    await sql`CREATE INDEX IF NOT EXISTS idx_a_dt ON appointments(datetime)`;
    await sql`CREATE INDEX IF NOT EXISTS idx_a_st ON appointments(final_status)`;
    await sql`CREATE INDEX IF NOT EXISTS idx_a_set ON appointments(setter_id)`;
    await sql`CREATE INDEX IF NOT EXISTS idx_act_c ON activities(created_at DESC)`;

    // Insert demo users
    await sql`
      INSERT INTO users (id, name, email, role, close_id) VALUES
        ('u1', 'Maxim Nickel', 'maxim@advisy.de', 'setter', 'usr_maxim'),
        ('u2', 'Sergej Janle', 'sergej@advisy.de', 'setter', 'usr_sergej'),
        ('u3', 'Tim Vogel', 'tim@advisy.de', 'admin', 'usr_tim'),
        ('u4', 'Pana', 'pana@advisy.de', 'opener', null),
        ('u5', 'Jaky', 'jaky@advisy.de', 'opener', null),
        ('u6', 'Sabine', 'sabine@advisy.de', 'opener', null)
      ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, role = EXCLUDED.role`;

    console.log("Tables created.");
    }
  }
}

// ─── Route Handler ───────────────────────────────────────────────────────
export default async function handler(req) {
  if (req.method === "OPTIONS") return new Response("", { status: 200, headers });

  await ensureTables();

  const url = new URL(req.url);
  // Path after /api/ - handle both direct and redirect paths
  const path = url.pathname.replace(/^\/(\.netlify\/functions\/api|api)\//, "/");
  const method = req.method;

  try {
    // ─── GET /users ────────────────────────────────────────────────
    if (path === "/users" && method === "GET") {
      const rows = await sql`SELECT * FROM users WHERE active = true ORDER BY role, name`;
      return json(rows);
    }

    // ─── GET /appointments ─────────────────────────────────────────
    if (path === "/appointments" && method === "GET") {
      const filter = url.searchParams.get("filter") || "all";
      const setter = url.searchParams.get("setter");
      const status = url.searchParams.get("status");
      const opener = url.searchParams.get("opener");
      const from = url.searchParams.get("from");
      const to = url.searchParams.get("to");

      let rows;
      if (filter === "today") {
        rows = await sql`SELECT * FROM appointments WHERE datetime::date = CURRENT_DATE ORDER BY datetime`;
      } else if (filter === "yesterday") {
        rows = await sql`SELECT * FROM appointments WHERE datetime::date = CURRENT_DATE - 1 ORDER BY datetime`;
      } else if (filter === "tomorrow") {
        rows = await sql`SELECT * FROM appointments WHERE datetime::date = CURRENT_DATE + 1 ORDER BY datetime`;
      } else if (filter === "week") {
        rows = await sql`SELECT * FROM appointments WHERE datetime >= date_trunc('week', CURRENT_DATE) AND datetime < date_trunc('week', CURRENT_DATE) + interval '7 days' ORDER BY datetime`;
      } else if (filter === "month") {
        rows = await sql`SELECT * FROM appointments WHERE datetime >= date_trunc('month', CURRENT_DATE) AND datetime < date_trunc('month', CURRENT_DATE) + interval '1 month' ORDER BY datetime`;
      } else if (from && to) {
        rows = await sql`SELECT * FROM appointments WHERE datetime >= ${from}::date AND datetime <= ${to}::date + interval '1 day' ORDER BY datetime`;
      } else {
        rows = await sql`SELECT * FROM appointments ORDER BY datetime DESC LIMIT 500`;
      }

      // Apply additional filters in JS (simpler than dynamic SQL)
      if (setter) rows = rows.filter(r => r.setter_id === setter);
      if (status) rows = rows.filter(r => r.final_status === status);
      if (opener) rows = rows.filter(r => r.opener_name === opener);

      return json(rows);
    }

    // ─── PATCH /appointments/:id ───────────────────────────────────
    if (path.startsWith("/appointments/") && method === "PATCH") {
      const id = path.split("/")[2];
      const body = await req.json();

      if (body.final_status) {
        await sql`UPDATE appointments SET
          final_status = ${body.final_status},
          status_at = now(),
          status_by = ${body.status_by || null},
          updated_at = now()
        WHERE id = ${id}`;
      }
      if (body.setter_id !== undefined) {
        await sql`UPDATE appointments SET
          setter_id = ${body.setter_id},
          updated_at = now()
        WHERE id = ${id}`;
      }

      const updated = await sql`SELECT * FROM appointments WHERE id = ${id}`;
      return json(updated[0] || {});
    }

    // ─── POST /appointments (for Close sync) ──────────────────────
    if (path === "/appointments" && method === "POST") {
      const body = await req.json();
      const items = Array.isArray(body) ? body : [body];
      let created = 0, updated = 0;

      for (const apt of items) {
        const result = await sql`
          INSERT INTO appointments (close_id, opener_name, setter_id, datetime, imported_status, source, lead_name, company, close_lead_id, notes, imported_at)
          VALUES (${apt.close_id}, ${apt.opener_name || 'Close'}, ${apt.setter_id || null}, ${apt.datetime}, ${apt.imported_status || 'scheduled'}, ${apt.source || 'close'}, ${apt.lead_name || ''}, ${apt.company || ''}, ${apt.close_lead_id || ''}, ${apt.notes || ''}, now())
          ON CONFLICT (close_id) DO UPDATE SET
            imported_status = EXCLUDED.imported_status,
            opener_name = EXCLUDED.opener_name,
            company = COALESCE(EXCLUDED.company, appointments.company),
            lead_name = COALESCE(EXCLUDED.lead_name, appointments.lead_name),
            updated_at = now()
          RETURNING (xmax = 0) as is_new`;
        if (result[0]?.is_new) created++; else updated++;
      }

      return json({ created, updated, total: items.length });
    }

    // ─── GET /activities ───────────────────────────────────────────
    if (path === "/activities" && method === "GET") {
      const limit = parseInt(url.searchParams.get("limit") || "30");
      const rows = await sql`SELECT * FROM activities ORDER BY created_at DESC LIMIT ${limit}`;
      return json(rows);
    }

    // ─── POST /activities ──────────────────────────────────────────
    if (path === "/activities" && method === "POST") {
      const body = await req.json();
      const result = await sql`INSERT INTO activities (msg, user_id) VALUES (${body.msg}, ${body.user_id || null}) RETURNING *`;
      return json(result[0]);
    }

    // ─── GET /kpis ─────────────────────────────────────────────────
    if (path === "/kpis" && method === "GET") {
      const today = await sql`
        SELECT
          count(*)::int as total,
          count(*) FILTER (WHERE final_status = 'open')::int as open,
          count(*) FILTER (WHERE final_status = 'show')::int as show,
          count(*) FILTER (WHERE final_status = 'no_show')::int as no_show,
          count(*) FILTER (WHERE final_status = 'rescheduled')::int as rescheduled
        FROM appointments WHERE datetime::date = CURRENT_DATE`;

      const week = await sql`
        SELECT
          count(*)::int as total,
          count(*) FILTER (WHERE final_status = 'open')::int as open,
          count(*) FILTER (WHERE final_status = 'show')::int as show,
          count(*) FILTER (WHERE final_status = 'no_show')::int as no_show,
          count(*) FILTER (WHERE final_status = 'rescheduled')::int as rescheduled
        FROM appointments WHERE datetime >= date_trunc('week', CURRENT_DATE) AND datetime < date_trunc('week', CURRENT_DATE) + interval '7 days'`;

      const month = await sql`
        SELECT
          count(*)::int as total,
          count(*) FILTER (WHERE final_status = 'open')::int as open,
          count(*) FILTER (WHERE final_status = 'show')::int as show,
          count(*) FILTER (WHERE final_status = 'no_show')::int as no_show,
          count(*) FILTER (WHERE final_status = 'rescheduled')::int as rescheduled
        FROM appointments WHERE datetime >= date_trunc('month', CURRENT_DATE) AND datetime < date_trunc('month', CURRENT_DATE) + interval '1 month'`;

      const rate = (s, n) => (s + n === 0) ? 0 : Math.round(s / (s + n) * 100);
      const fmt = r => ({ ...r, rate: rate(r.show, r.no_show) });

      return json({ today: fmt(today[0]), week: fmt(week[0]), month: fmt(month[0]) });
    }

    // ─── GET /setter-perf ──────────────────────────────────────────
    if (path === "/setter-perf" && method === "GET") {
      const rows = await sql`
        SELECT
          u.id, u.name,
          count(a.id) FILTER (WHERE a.datetime::date = CURRENT_DATE)::int as today_total,
          count(a.id) FILTER (WHERE a.datetime::date = CURRENT_DATE AND a.final_status = 'show')::int as today_show,
          count(a.id) FILTER (WHERE a.datetime::date = CURRENT_DATE AND a.final_status = 'no_show')::int as today_no_show,
          count(a.id) FILTER (WHERE a.datetime::date = CURRENT_DATE AND a.final_status = 'rescheduled')::int as today_rescheduled,
          count(a.id) FILTER (WHERE a.datetime::date = CURRENT_DATE AND a.final_status = 'open')::int as today_open,
          count(a.id) FILTER (WHERE a.datetime >= date_trunc('week', CURRENT_DATE) AND a.final_status = 'show')::int as week_show,
          count(a.id) FILTER (WHERE a.datetime >= date_trunc('week', CURRENT_DATE) AND a.final_status = 'no_show')::int as week_no_show,
          count(a.id) FILTER (WHERE a.datetime >= date_trunc('month', CURRENT_DATE) AND a.final_status = 'show')::int as month_show,
          count(a.id) FILTER (WHERE a.datetime >= date_trunc('month', CURRENT_DATE) AND a.final_status = 'no_show')::int as month_no_show
        FROM users u
        LEFT JOIN appointments a ON a.setter_id = u.id
        WHERE u.role = 'setter' AND u.active = true
        GROUP BY u.id, u.name
        ORDER BY u.name`;

      const rate = (s, n) => (s + n === 0) ? 0 : Math.round(s / (s + n) * 100);
      return json(rows.map(r => ({
        ...r,
        today_rate: rate(r.today_show, r.today_no_show),
        week_rate: rate(r.week_show, r.week_no_show),
        month_rate: rate(r.month_show, r.month_no_show),
      })));
    }

    // ─── GET /opener-perf ──────────────────────────────────────────
    if (path === "/opener-perf" && method === "GET") {
      const rows = await sql`
        SELECT
          opener_name as name,
          count(*)::int as total,
          count(*) FILTER (WHERE final_status = 'show')::int as show,
          count(*) FILTER (WHERE final_status = 'no_show')::int as no_show,
          count(*) FILTER (WHERE final_status = 'rescheduled')::int as rescheduled
        FROM appointments
        WHERE opener_name IS NOT NULL
        GROUP BY opener_name
        ORDER BY count(*) DESC`;

      const rate = (s, n) => (s + n === 0) ? 0 : Math.round(s / (s + n) * 100);
      return json(rows.map(r => ({ ...r, rate: rate(r.show, r.no_show) })));
    }

    // ─── GET /chart-data ───────────────────────────────────────────
    if (path === "/chart-data" && method === "GET") {
      const rows = await sql`
        SELECT
          datetime::date as day,
          count(*)::int as total,
          count(*) FILTER (WHERE final_status = 'show')::int as show,
          count(*) FILTER (WHERE final_status = 'no_show')::int as no_show,
          count(*) FILTER (WHERE final_status = 'rescheduled')::int as rescheduled
        FROM appointments
        WHERE datetime >= CURRENT_DATE - interval '14 days'
        GROUP BY datetime::date
        ORDER BY day`;

      const rate = (s, n) => (s + n === 0) ? 0 : Math.round(s / (s + n) * 100);
      return json(rows.map(r => ({
        ...r,
        label: new Date(r.day).toLocaleDateString("de-DE", { day: "2-digit", month: "2-digit" }),
        rate: rate(r.show, r.no_show),
      })));
    }

    // ─── POST /sync-close (Close API → DB) ─────────────────────────
    if (path === "/sync-close" && method === "POST") {
      const CLOSE_KEY = "api_5Fh9iC7MmeCy5urgaR2hDX.1b7xYKAKNPrsDkA5SaOUcb";
      const auth = "Basic " + Buffer.from(CLOSE_KEY + ":").toString("base64");
      const closeGet = async (endpoint) => {
        const r = await fetch("https://api.close.com/api/v1" + endpoint, { headers: { Authorization: auth } });
        if (!r.ok) throw new Error("Close " + r.status);
        return r.json();
      };

      // Setter mapping: Close user_name → internal user_id
      const SETTER_MAP = {
        "Maxim Nickel": "u1",
        "Sergej Janle": "u2",
        "Tim Vogel": "u3",
      };

      // 1. Fetch meetings
      const meetData = await closeGet("/activity/meeting/?_limit=200&_order_by=-date_created");
      const meetings = meetData.data || [];

      // 2. Collect unique lead_ids to find openers
      const leadIds = [...new Set(meetings.map(m => m.lead_id).filter(Boolean))];

      // 3. For each lead, find who changed status to "Setting" = Opener
      // Fetch LeadStatusChange activities and get Close users for name lookup
      const openerCache = {}; // lead_id → opener_name

      // Fetch Close users for ID→name mapping
      const closeUsersData = await closeGet("/user/");
      const closeUsers = {};
      for (const u of (closeUsersData.data || [])) {
        closeUsers[u.id] = (u.first_name + " " + u.last_name).trim();
      }

      // Batch fetch status changes for leads (max 50 at a time to avoid rate limits)
      for (let i = 0; i < leadIds.length; i += 10) {
        const batch = leadIds.slice(i, i + 10);
        for (const leadId of batch) {
          try {
            const scData = await closeGet("/activity/leadstatuschange/?lead_id=" + leadId + "&_limit=20&_order_by=-date_created");
            const changes = scData.data || [];
            // Find the status change TO "Setting"
            for (const sc of changes) {
              if (sc.new_status_label === "Setting" || (sc.new_status && sc.new_status.toLowerCase().includes("setting"))) {
                const openerUserId = sc.user_id;
                openerCache[leadId] = closeUsers[openerUserId] || sc.user_name || "Unbekannt";
                break;
              }
            }
          } catch (e) {
            // Skip this lead if lookup fails
          }
        }
        // Small delay between batches
        if (i + 10 < leadIds.length) await new Promise(r => setTimeout(r, 300));
      }

      // 4. Upsert meetings with correct setter + opener
      let created = 0, updated = 0;
      for (const m of meetings) {
        if (!m.id) continue;
        let is = "scheduled";
        if (m.status === "completed" || m.status === "done") is = "completed";
        if (m.status === "canceled" || m.status === "cancelled") is = "cancelled";

        // Setter = meeting owner from Close
        const closeUserName = m.user_name || m._user_name || "";
        const setterId = SETTER_MAP[closeUserName] || null;

        // Opener = who changed lead status to "Setting"
        const openerName = openerCache[m.lead_id] || "–";

        const result = await sql`
          INSERT INTO appointments (close_id, opener_name, setter_id, datetime, imported_status, source, lead_name, company, close_lead_id, notes, imported_at)
          VALUES (${m.id}, ${openerName}, ${setterId}, ${m.starts_at || m.date_created}, ${is}, 'close', ${m.lead_name || ''}, ${m.title || m.note?.substring(0, 80) || 'Close Meeting'}, ${m.lead_id || ''}, ${m.note || ''}, now())
          ON CONFLICT (close_id) DO UPDATE SET
            imported_status = EXCLUDED.imported_status,
            opener_name = COALESCE(NULLIF(EXCLUDED.opener_name, '–'), appointments.opener_name),
            setter_id = COALESCE(EXCLUDED.setter_id, appointments.setter_id),
            company = COALESCE(EXCLUDED.company, appointments.company),
            updated_at = now()
          RETURNING (xmax = 0) as is_new`;
        if (result[0]?.is_new) created++; else updated++;
      }

      await sql`INSERT INTO activities (msg, user_id) VALUES (${'Close Sync: ' + created + ' neu, ' + updated + ' aktualisiert (mit Opener-Lookup)'}, 'system')`;

      return json({ created, updated, total: meetings.length, openers_found: Object.keys(openerCache).length });
    }

    // ─── POST /seed-demo ───────────────────────────────────────────
    if (path === "/seed-demo" && method === "POST") {
      await sql`DELETE FROM activities`;
      await sql`DELETE FROM appointments`;

      const ops = ["Opener A", "Opener B", "Opener C"];
      const firms = ["TechFlow GmbH", "BrandHaus AG", "NordStyle KG", "DigitalPeak GmbH", "FreshCommerce AG", "MediaStack GmbH", "FlowRetail KG", "UrbanBrand GmbH", "NextLevel AG", "PureCommerce GmbH"];
      const setterIds = ["u1", "u2", "u3"];
      const now = new Date();

      for (let off = -14; off <= 3; off++) {
        const n = 2 + Math.floor(Math.random() * 5);
        for (let j = 0; j < n; j++) {
          const d = new Date(now); d.setDate(d.getDate() + off);
          d.setHours(9 + Math.floor(Math.random() * 9), Math.random() > .5 ? 0 : 30, 0, 0);
          const firm = firms[Math.floor(Math.random() * firms.length)];
          const sid = setterIds[Math.floor(Math.random() * 3)];
          const opener = ops[Math.floor(Math.random() * 3)];
          let fs = "open", is = "scheduled";
          if (off < 0) { const r = Math.random(); fs = r < .55 ? "show" : r < .8 ? "no_show" : r < .92 ? "rescheduled" : "cancelled"; is = fs === "show" ? "completed" : "scheduled"; }
          else if (off === 0) { const r = Math.random(); fs = r < .28 ? "show" : r < .42 ? "no_show" : r < .52 ? "rescheduled" : "open"; }

          await sql`INSERT INTO appointments (opener_name, setter_id, datetime, imported_status, final_status, source, lead_name, company, status_at, status_by)
            VALUES (${opener}, ${sid}, ${d.toISOString()}, ${is}, ${fs}, 'demo', ${"AP " + firm.split(" ")[0]}, ${firm}, ${fs !== "open" ? d.toISOString() : null}, ${fs !== "open" ? sid : null})`;
        }
      }

      await sql`INSERT INTO activities (msg, user_id) VALUES ('Demo-Daten generiert', 'system')`;
      return json({ ok: true });
    }

    return err("Not found: " + path, 404);

  } catch (e) {
    console.error("API Error:", e);
    return err(e.message || "Internal Server Error", 500);
  }
}

export const config = { path: ["/api/*", "/.netlify/functions/api/*"] };
