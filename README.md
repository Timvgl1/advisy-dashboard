# Advisy Setter Dashboard

## Deploy in 5 Minuten

### 1. Repo auf GitHub pushen
```bash
git init
git add .
git commit -m "Advisy Dashboard"
git remote add origin https://github.com/DEIN-USER/advisy-dashboard.git
git push -u origin main
```

### 2. Netlify verbinden
- netlify.com → New Site → Import from Git → dein Repo wählen
- Build command: leer lassen
- Publish directory: `public`
- Deploy klicken

### 3. Netlify DB aktivieren
```bash
npm install
npx netlify db init
```
Oder: Netlify UI → Extensions → Neon Database → Add Database

### 4. Erste Daten laden
Nach dem Deploy: Öffne dein Dashboard → Login als Admin → "Demo-Daten laden"
Oder: "Close Sync" klicken um echte Close-Termine zu importieren.

### 5. Link ans Team schicken
Deine URL: `https://advisy-dashboard.netlify.app` (oder custom domain)

## Projektstruktur
```
├── public/
│   └── index.html          ← Dashboard Frontend
├── netlify/
│   └── functions/
│       └── api.mjs          ← API (DB, Close Sync, KPIs)
├── netlify.toml              ← Netlify Config
└── package.json
```

## API Endpoints
- `GET  /api/users` – Alle User
- `GET  /api/appointments?filter=today` – Termine (today/week/month/all)
- `PATCH /api/appointments/:id` – Status Update
- `POST /api/sync-close` – Close CRM Sync (Server-seitig, kein CORS)
- `GET  /api/kpis` – KPI Berechnung
- `GET  /api/setter-perf` – Setter Performance
- `GET  /api/chart-data` – Chart Daten (14 Tage)
- `POST /api/seed-demo` – Demo-Daten generieren

## Close Sync
Der Close Sync läuft über die Netlify Function – kein CORS Problem.
Dein API Key ist im Code. Für Production: als Environment Variable setzen.
