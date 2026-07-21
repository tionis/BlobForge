export const DOCS_VERSION = "1";

export function renderDocs(): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="theme-color" content="#f4f6f2">
  <meta name="description" content="BlobForge is a self-hosted, distributed pipeline for converting PDF libraries into Markdown and structured assets.">
  <title>BlobForge - Distributed PDF conversion</title>
  <link rel="icon" href="/static/blobforge-v1.svg" type="image/svg+xml">
  <link rel="stylesheet" href="/static/docs-v${DOCS_VERSION}.css">
</head>
<body>
  <header class="site-header">
    <a class="brand" href="/" aria-label="BlobForge home"><img src="/static/blobforge-v1.svg" alt="" width="34" height="34"><strong>BlobForge</strong></a>
    <nav aria-label="Documentation">
      <a href="#overview">Overview</a>
      <a href="#install">Install</a>
      <a href="#operate">Operate</a>
      <a href="#reference">Reference</a>
    </nav>
    <a class="console-link" href="/console">Open console</a>
  </header>

  <main>
    <section class="intro" id="overview">
      <div class="intro-copy">
        <p class="kicker">Self-hosted document infrastructure</p>
        <h1>BlobForge</h1>
        <p class="lede">Convert large PDF collections into searchable Markdown without tying storage, coordination, and compute to one machine.</p>
        <div class="intro-actions"><a class="primary" href="#install">Install a worker</a><a class="secondary" href="https://github.com/tionis/BlobForge">View source</a></div>
      </div>
      <div class="pipeline" aria-label="BlobForge processing pipeline">
        <div><span>01</span><strong>Ingest</strong><p>Hash and upload each PDF once.</p></div>
        <div><span>02</span><strong>Coordinate</strong><p>Queue jobs with priorities and fenced leases.</p></div>
        <div><span>03</span><strong>Convert</strong><p>Run Marker on independent Linux workers.</p></div>
        <div><span>04</span><strong>Retrieve</strong><p>Store Markdown and extracted assets as ZIP output.</p></div>
      </div>
    </section>

    <section class="summary-band">
      <div><strong>Content addressed</strong><p>SHA-256 identity prevents duplicate uploads and conversions.</p></div>
      <div><strong>Scale to zero</strong><p>The coordinator is stateless at the edge; workers can stop when idle.</p></div>
      <div><strong>Least privilege</strong><p>Each worker gets its own credential and short-lived object URLs.</p></div>
    </section>

    <section class="section" id="install">
      <div class="section-heading"><p class="kicker">Worker guide</p><h2>Bring a Linux machine online</h2><p>Workers need Podman or Docker, a systemd user session, and an enrollment token created in the administration console. No repository clone or Python environment is required.</p></div>
      <ol class="steps">
        <li><span>1</span><div><h3>Create an enrollment</h3><p>Open the console, create a worker with a unique label, and keep the one-time token. A token belongs to exactly one worker identity.</p></div></li>
        <li><span>2</span><div><h3>Run the installer</h3><pre><code>curl -fsSLO https://raw.githubusercontent.com/tionis/BlobForge/main/scripts/install-linux-worker.sh
chmod +x install-linux-worker.sh
./install-linux-worker.sh \
  --coordinator-url https://blobforge.example \
  --token bfw_REPLACE_ME</code></pre></div></li>
        <li><span>3</span><div><h3>Verify the service</h3><pre><code>systemctl --user status blobforge-worker
journalctl --user -u blobforge-worker -f</code></pre><p>The installer stores credentials with mode 0600 and persists downloaded models under <code>~/.cache/blobforge</code>.</p></div></li>
      </ol>
      <div class="callout"><strong>CPU and CUDA images</strong><p>The default multi-architecture image is CPU-only. Add <code>--gpu</code> on a configured NVIDIA host to select the amd64 CUDA image and pass through the GPU.</p></div>
    </section>

    <section class="section alternate" id="operate">
      <div class="section-heading"><p class="kicker">Operations</p><h2>Control when conversion runs</h2><p>Run windows use the worker machine's local time. Outside a window the worker publishes one suspended transition, sends no periodic heartbeats, and sleeps until work is allowed again.</p></div>
      <div class="guide-grid">
        <article><h3>Night-only worker</h3><pre><code>./install-linux-worker.sh \
  --coordinator-url https://blobforge.example \
  --token bfw_REPLACE_ME \
  --run-window 22:00-06:00</code></pre><p>New jobs are claimed only between 22:00 and 06:00. A conversion already running at 06:00 is allowed to finish.</p></article>
        <article><h3>Hard schedule boundary</h3><pre><code>./install-linux-worker.sh \
  --coordinator-url https://blobforge.example \
  --token bfw_REPLACE_ME \
  --run-window 22:00-06:00 \
  --abort-outside-window</code></pre><p>The active conversion is terminated at the boundary and safely requeued. Conversion isolation is enabled automatically.</p></article>
        <article><h3>Service lifecycle</h3><pre><code>systemctl --user restart blobforge-worker
systemctl --user stop blobforge-worker
podman pull ghcr.io/tionis/blobforge:latest</code></pre><p>Enable user lingering with <code>loginctl enable-linger</code> when the worker must run without an interactive login.</p></article>
        <article><h3>Failure recovery</h3><p>Workers hold fenced leases and renew them while processing. Catchable shutdowns requeue active work immediately; expired leases are recovered before the next claim. Native converter crashes stay contained when isolation is enabled.</p></article>
      </div>
    </section>

    <section class="section" id="reference">
      <div class="section-heading"><p class="kicker">Runtime reference</p><h2>Coordinator policy</h2><p>Administrators edit these values in the console. Workers receive updates on registration, claims, and heartbeats.</p></div>
      <div class="table-wrap"><table><thead><tr><th>Setting</th><th>Default</th><th>Behavior</th></tr></thead><tbody>
        <tr><td><code>heartbeat_enabled</code></td><td>true</td><td>Send idle and prompt progress updates. Lease renewal continues when disabled.</td></tr>
        <tr><td><code>heartbeat_interval</code></td><td>60 seconds</td><td>Normal heartbeat cadence. A changed value applies after the next coordinator response.</td></tr>
        <tr><td><code>lease_seconds</code></td><td>900 seconds</td><td>Processing lease duration. Lease-only mode renews one-third of the way to expiry.</td></tr>
        <tr><td><code>max_retries</code></td><td>3</td><td>Failed attempts allowed before a job enters the dead-letter queue.</td></tr>
        <tr><td><code>conversion_timeout</code></td><td>3600 seconds</td><td>Maximum conversion runtime where platform timer enforcement is available.</td></tr>
      </tbody></table></div>
    </section>

    <section class="section faq">
      <div class="section-heading"><p class="kicker">Help</p><h2>Common questions</h2></div>
      <details><summary>Where are PDFs and results stored?</summary><p>PDF and ZIP bodies stay in the configured S3-compatible object store. Bunny Database stores file metadata, queue state, leases, worker records, configuration, failure history, and audit events.</p></details>
      <details><summary>What happens when a worker disappears?</summary><p>Its lease eventually expires. Recovery records a failed attempt and makes the job claimable again, subject to retry policy.</p></details>
      <details><summary>Can several workers use one token?</summary><p>No. Enrollment tokens are bound to one worker ID. Create a separate enrollment for every machine.</p></details>
      <details><summary>How do I inspect a failed conversion?</summary><p>Open the file in the administration console and select Failures. Each failed attempt retains its worker, stage, traceback, context, and latest progress snapshot.</p></details>
    </section>
  </main>

  <footer><div><strong>BlobForge</strong><p>Distributed PDF conversion with independently managed storage, coordination, and compute.</p></div><div><a href="https://github.com/tionis/BlobForge">GitHub</a><a href="/login">Administrator sign in</a></div></footer>
</body>
</html>`;
}

export const DOCS_CSS = `
:root{color-scheme:light;--paper:#f4f6f2;--surface:#fff;--ink:#18201d;--muted:#5d6964;--line:#cbd3ce;--green:#177e68;--green-dark:#0e5547;--amber:#e9b949;--coral:#d95d4f;--night:#172521;font-family:Inter,ui-sans-serif,system-ui,sans-serif}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--paper);color:var(--ink);font-size:16px;line-height:1.55}a{color:inherit}code,pre{font-family:"SFMono-Regular",Consolas,"Liberation Mono",monospace}.site-header{position:sticky;z-index:10;top:0;display:grid;grid-template-columns:1fr auto 1fr;align-items:center;gap:24px;min-height:68px;padding:0 max(24px,calc((100% - 1180px)/2));border-bottom:1px solid var(--line);background:#f4f6f2f2;backdrop-filter:blur(12px)}.brand{display:flex;align-items:center;gap:10px;text-decoration:none}.brand strong{font-size:1.05rem}.site-header nav{display:flex;gap:24px}.site-header nav a{color:var(--muted);font-size:.86rem;text-decoration:none}.site-header nav a:hover{color:var(--ink)}.console-link{justify-self:end;padding:9px 13px;border:1px solid var(--ink);border-radius:5px;font-size:.82rem;font-weight:700;text-decoration:none}.console-link:hover{background:var(--ink);color:#fff}.intro{width:min(1180px,calc(100% - 48px));min-height:calc(100vh - 112px);margin:auto;padding:90px 0 56px;display:grid;grid-template-columns:minmax(0,1fr) minmax(480px,.9fr);align-items:center;gap:70px}.kicker{margin:0 0 12px;color:var(--green-dark);font-size:.72rem;font-weight:800;letter-spacing:.14em;text-transform:uppercase}.intro h1{margin:0;font-size:72px;line-height:1;letter-spacing:0}.lede{max-width:650px;margin:24px 0 0;color:var(--muted);font-size:1.28rem;line-height:1.55}.intro-actions{display:flex;gap:10px;margin-top:32px}.intro-actions a{padding:11px 16px;border-radius:5px;font-size:.88rem;font-weight:750;text-decoration:none}.primary{background:var(--green-dark);color:#fff}.secondary{border:1px solid var(--line);background:var(--surface)}.pipeline{display:grid;grid-template-columns:1fr 1fr;border:1px solid var(--night);background:var(--night);gap:1px}.pipeline div{min-height:185px;padding:24px;background:#f8faf7}.pipeline div:nth-child(2){border-top:5px solid var(--amber)}.pipeline div:nth-child(3){border-left:5px solid var(--coral)}.pipeline div:nth-child(4){background:#e1eee9}.pipeline span{display:block;color:var(--muted);font-size:.7rem;font-weight:800}.pipeline strong{display:block;margin-top:24px;font-size:1.1rem}.pipeline p{margin:8px 0 0;color:var(--muted);font-size:.86rem}.summary-band{display:grid;grid-template-columns:repeat(3,1fr);background:var(--night);color:#fff}.summary-band div{padding:32px max(24px,calc((100vw - 1180px)/6));border-right:1px solid #ffffff24}.summary-band div:last-child{border:0}.summary-band p{margin:7px 0 0;color:#bed0c9;font-size:.86rem}.section{padding:96px max(24px,calc((100% - 1180px)/2));scroll-margin-top:68px}.section.alternate{background:#e7ece8}.section-heading{display:grid;grid-template-columns:1fr 1fr;gap:40px;align-items:end;margin-bottom:42px}.section-heading .kicker{grid-column:1/-1;margin-bottom:-24px}.section-heading h2{margin:0;font-size:40px;line-height:1.15;letter-spacing:0}.section-heading>p:last-child{margin:0;color:var(--muted)}.steps{list-style:none;margin:0;padding:0;border-top:1px solid var(--line)}.steps li{display:grid;grid-template-columns:55px minmax(0,1fr);gap:20px;padding:30px 0;border-bottom:1px solid var(--line)}.steps li>span{display:grid;place-items:center;width:38px;height:38px;border-radius:50%;background:var(--green-dark);color:#fff;font-size:.8rem;font-weight:800}.steps h3,.guide-grid h3{margin:4px 0 10px;font-size:1rem}.steps p,.guide-grid p{margin:10px 0 0;color:var(--muted)}pre{max-width:100%;overflow:auto;margin:14px 0 0;padding:18px;border-left:4px solid var(--green);background:var(--night);color:#eaf4ef;font-size:.78rem;line-height:1.6}p code,td code{padding:2px 5px;border:1px solid var(--line);border-radius:3px;background:#fff;font-size:.83em}.callout{display:grid;grid-template-columns:220px 1fr;gap:30px;margin-top:32px;padding:24px;border-left:5px solid var(--amber);background:#fff}.callout p{margin:0;color:var(--muted)}.guide-grid{display:grid;grid-template-columns:1fr 1fr;border-top:1px solid var(--line);border-left:1px solid var(--line)}.guide-grid article{min-width:0;padding:28px;border-right:1px solid var(--line);border-bottom:1px solid var(--line);background:#f7f9f6}.table-wrap{overflow:auto;border:1px solid var(--line);background:#fff}table{width:100%;border-collapse:collapse;font-size:.88rem}th,td{padding:16px 18px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top}th{background:#e6ece8;font-size:.7rem;letter-spacing:.1em;text-transform:uppercase}tr:last-child td{border:0}td:nth-child(2){white-space:nowrap;color:var(--green-dark);font-weight:700}.faq{padding-top:42px}.faq details{border-top:1px solid var(--line);padding:20px 0}.faq details:last-child{border-bottom:1px solid var(--line)}.faq summary{cursor:pointer;font-weight:750}.faq details p{max-width:760px;margin:12px 0 0;color:var(--muted)}footer{display:flex;justify-content:space-between;gap:32px;padding:48px max(24px,calc((100% - 1180px)/2));background:var(--night);color:#fff}footer p{max-width:540px;margin:8px 0 0;color:#bed0c9;font-size:.84rem}footer>div:last-child{display:flex;gap:24px;align-items:flex-start}footer a{color:#fff;font-size:.84rem}@media(max-width:860px){.site-header{grid-template-columns:1fr auto}.site-header nav{display:none}.intro{min-height:auto;padding-top:62px;grid-template-columns:1fr}.intro h1{font-size:54px}.pipeline{max-width:620px}.summary-band{grid-template-columns:1fr}.summary-band div{padding:24px;border-right:0;border-bottom:1px solid #ffffff24}.section{padding-top:68px;padding-bottom:68px}.section-heading{grid-template-columns:1fr;gap:14px}.section-heading .kicker{grid-column:auto;margin-bottom:0}.guide-grid{grid-template-columns:1fr}.callout{grid-template-columns:1fr}footer{flex-direction:column}}@media(max-width:540px){.site-header{padding:0 16px}.brand img{width:30px;height:30px}.console-link{padding:8px 10px}.intro{width:calc(100% - 32px);padding-top:44px;gap:42px}.intro h1{font-size:44px}.lede{font-size:1.05rem}.intro-actions{align-items:stretch;flex-direction:column}.intro-actions a{text-align:center}.pipeline{grid-template-columns:1fr}.pipeline div{min-height:140px}.pipeline div:nth-child(3){border-left:0;border-top:5px solid var(--coral)}.section{padding-left:16px;padding-right:16px}.section-heading h2{font-size:32px}.steps li{grid-template-columns:42px minmax(0,1fr);gap:12px}.steps li>span{width:34px;height:34px}footer{padding-left:16px;padding-right:16px}footer>div:last-child{flex-direction:column;gap:10px}}
@media(max-width:540px){.intro{padding:32px 0 28px;gap:28px}.lede{margin-top:16px}.intro-actions{margin-top:22px;align-items:initial;flex-direction:row}.pipeline{grid-template-columns:1fr 1fr}.pipeline div{min-height:90px;padding:15px}.pipeline strong{margin-top:12px;font-size:.95rem}.pipeline p{display:none}.pipeline div:nth-child(3){border-left:5px solid var(--coral);border-top:0}}
`;

export const BRAND_SVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" role="img" aria-label="BlobForge"><rect width="64" height="64" rx="8" fill="#172521"/><path d="M15 16h22l12 12v20H15z" fill="#f4f6f2"/><path d="M37 16v13h12" fill="none" stroke="#177e68" stroke-width="5"/><path d="M22 36h20M22 43h14" stroke="#d95d4f" stroke-width="4"/></svg>`;

export const ROBOTS_TXT = `User-agent: *
Allow: /
Disallow: /api/
Disallow: /auth/
Disallow: /console
Disallow: /login
`;
