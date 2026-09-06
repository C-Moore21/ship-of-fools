// Minimal static server that reproduces Flask's /beta route from the Vite
// manifest, so the bench can measure the *shipped* bundle without booting
// Flask or touching MongoDB. In --server=flask mode the runner skips this and
// points at localhost:5000 instead.
import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..')
const BETA_DIR = path.join(ROOT, 'static', 'beta')

const MIME = {
  '.js': 'text/javascript', '.css': 'text/css', '.html': 'text/html',
  '.json': 'application/json', '.svg': 'image/svg+xml', '.png': 'image/png',
  '.jpg': 'image/jpeg', '.webp': 'image/webp', '.woff2': 'font/woff2',
  '.ico': 'image/x-icon', '.map': 'application/json',
}

function entryFromManifest() {
  const mf = path.join(BETA_DIR, '.vite', 'manifest.json')
  if (!fs.existsSync(mf)) throw new Error(`No manifest at ${mf} — run: cd beta && npm run build`)
  const m = JSON.parse(fs.readFileSync(mf, 'utf8'))
  const entry = Object.values(m).find((e) => e.isEntry) ?? m['index.html']
  if (!entry) throw new Error('No isEntry in manifest')
  return {
    js: `/static/beta/${entry.file}`,
    css: entry.css?.length ? `/static/beta/${entry.css[0]}` : null,
  }
}

function betaHtml() {
  const { js, css } = entryFromManifest()
  // Mirrors templates/beta.html so bundle/CSS ordering matches production.
  return `<!doctype html>
<html lang="en"><head>
<meta charset="UTF-8" /><meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Ship of Fools — Beta</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="preload" as="style" href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700;900&family=Space+Mono:wght@400;700&display=swap">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700;900&family=Space+Mono:wght@400;700&display=swap">
${css ? `<link rel="preload" as="style" href="${css}">\n<link rel="stylesheet" href="${css}">` : ''}
<link rel="modulepreload" href="${js}">
</head><body><div id="root"></div>
<script type="module" src="${js}"></script>
</body></html>`
}

export function startServer(port = 5199) {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://localhost')
    const p = url.pathname

    if (p === '/beta' || p === '/beta/' || p === '/') {
      const body = betaHtml()
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' })
      return res.end(body)
    }

    if (p.startsWith('/static/beta/')) {
      const rel = p.slice('/static/beta/'.length)
      const file = path.join(BETA_DIR, rel)
      if (!file.startsWith(BETA_DIR) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
        res.writeHead(404); return res.end('not found')
      }
      res.writeHead(200, {
        'content-type': MIME[path.extname(file)] ?? 'application/octet-stream',
        'cache-control': 'no-store',
      })
      return fs.createReadStream(file).pipe(res)
    }

    // /api/* is fulfilled by Playwright route interception; anything reaching
    // here in replay mode is a fixture miss worth seeing.
    res.writeHead(404, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ error: 'bench-static-server: unhandled', path: p }))
  })

  return new Promise((resolve, reject) => {
    server.on('error', reject)
    server.listen(port, '127.0.0.1', () => resolve({ server, origin: `http://127.0.0.1:${port}` }))
  })
}
