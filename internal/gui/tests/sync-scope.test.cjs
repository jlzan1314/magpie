// Run with Node's test runner and Playwright on the module path; see README.md.
// What syncs, a preview of a sync, and undoing the last one (#6428,
// jinpwng on X: sync only the library and memory, leave each computer's
// providers, settings and models alone, see what an apply changes first,
// and roll it back):
// - The form's What syncs: Providers, With their API keys under it,
//   Settings, Agents' models, Library and Memory up front; Profiles, Usage
//   and Sign-ins under Advanced, folded. Providers unticked hides the keys
//   tick. Save posts providers and settings false, and nothing for a part
//   left as it was (an old setup is saved as it was).
// - Saved, the line under the sync says what stays on each computer.
// - Preview… shows what comes here and what goes up, by item, and Sync now
//   sends the preview's token; a stale one is shown again, as it is now.
// - Undo the last sync asks first; a part changed here since is named, and
//   Undo anyway sends force.
// In all five languages, at 1100 and 420px, Chromium and WebKit, nothing
// wider than the window. The API is faked; nothing is synced.
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const path = require("node:path");
const { test } = require("node:test");
const { chromium, webkit } = require("playwright");
const { reach } = require("./reader.cjs");
const { strings } = require("./strings.cjs");

const assets = path.resolve(__dirname, "../assets");
const { I18N } = strings();
const tr = (lang, k, vars = {}) => {
  const s = lang === "en" ? k : I18N[lang][k];
  assert.equal(typeof s, "string", `${lang} has ${JSON.stringify(k)}`);
  return s.replace(/\{(\w+)\}/g, (_, v) => vars[v]);
};

const minAgo = new Date(Date.now() - 60000).toISOString();
const longSessionError = "Could not read the session " + "unbroken-session-id".repeat(18);
const base = { on: true, kind: "webdav", url: "https://dav.example.invalid/dav/", user: "me", passwordSet: true, passphraseSet: true, keys: true, agents: true, library: true, memory: true, last: minAgo, applied: { at: minAgo, parts: ["library"] } };
const plan = (token) => ({
  token,
  parts: [
    { part: "providers", do: "off" },
    { part: "settings", do: "off" },
    { part: "profiles", do: "same" },
    { part: "agents", do: "same" },
    { part: "library", do: "in", replaces: true, items: [
      { kind: "skills", name: "review", change: "added" },
      { kind: "servers", name: "github", change: "changed" },
      ...Array.from({ length: 9 }, (_, i) => ({ kind: "skills", name: "old-" + i, change: "removed" })),
    ] },
  ],
  memory: { in: 2, up: 1 },
});

function serve(lang, posts, remoteGate) {
  const theme = process.env.MAGPIE_TEST_THEME || "light";
  const settings = {
    theme, lang, tray: "panel", version: "test", dir: "/tmp/magpie",
    gateway: "http://127.0.0.1:3425", visionModels: [], imageGenModels: [],
    fx: { rate: 7.2, stale: false },
  };
  let sync = { ...base, sessionsError: longSessionError };
  let activity = {phase:"idle",error:longSessionError};
  let tokens = 0, previewed = 0, undos = 0, sessionPrefs = { all: true };
  return async (route) => {
    const request = route.request();
    const pathname = new URL(request.url()).pathname;
    const json = (data, status = 200) => route.fulfill({ status, json: data });
    const post = () => { const body = request.postDataJSON(); posts.push({ path: pathname, body }); return body; };
    if (pathname === "/boot.js") return route.fulfill({ contentType: "text/javascript", body: `window.bootPrefs = ${JSON.stringify({lang, theme, web:false})};` });
    if (pathname === "/wails/runtime.js") return route.fulfill({ contentType: "text/javascript", body: "export const Window = {};" });
    if (pathname === "/api/state") return json({ agents: [], profiles: [], settings });
    if (pathname === "/api/settings") return json(settings);
    if (pathname === "/api/usage/quotas") return json([]);
    if (pathname === "/api/groups") return json({ groups: [], models: [] });
    if (pathname === "/api/plugins") return json({ plugins: [] });
    if (pathname === "/api/davsync") return json(new URL(request.url()).searchParams.get("view") === "session-activity" ? activity : sync);
    if (pathname === "/api/davsync/session-candidates") return json({
      sources: [
        { agent: "codex", id: "sample", title: "Long title ".repeat(18), cwd: "/project/" + "directory".repeat(28) },
        { agent: "claude", id: "other", title: "Another conversation", cwd: "/project" },
      ],
      targets: ["codex"],
      preferences: sessionPrefs,
    });
    if (pathname === "/api/davsync/session-preferences") {
      sessionPrefs = post();
      return route.fulfill({ status: 204 });
    }
    if (pathname === "/api/davsync/session-sync-start") {
      post();
      activity=activity.phase==="waiting" ? {phase:"done",latest:"example",last:minAgo,conversations:1,uploaded:0,skipped:2} : {phase:"waiting",retryAt:new Date(Date.now()+30000).toISOString(),canRetry:true};
      return json(activity);
    }
    if (pathname === "/api/davsync/session-restore-choices") {
      const body=post();
      if (body.scope==="webdav") await remoteGate;
      if (body.scope) return json({items:body.scope==="local" ? [{id:"snapshot",local:true,updated:minAgo}] : [],canDeleteWebDAV:true});
      return json({id:"snapshot",local:true,ready:!!body.verify,device:"Laptop",entries:[{id:"thread",title:"Restore fixture"}]});
    }
    if (pathname === "/api/davsync/session-batch") {post();return json({conversations:1});}
    if (pathname === "/api/davsync/save") {
      const b = post();
      sync = { ...sync };
      for (const k of ["providers", "settings", "profiles"]) if (k in b) sync[k] = b[k];
      return json(sync);
    }
    if (pathname === "/api/davsync/preview") { post(); return json(plan("t" + ++tokens)); }
    if (pathname === "/api/davsync/sync-previewed") {
      post();
      // the first is stale: the server changed since the preview
      if (++previewed === 1) return json({ error: "something changed since the preview: look at it again before syncing" }, 409);
      sync = { ...sync, last: new Date().toISOString() };
      return json(sync);
    }
    if (pathname === "/api/davsync/undo-sync") {
      post();
      if (++undos === 1) return json({ error: "changed here since the sync", changed: ["library"] }, 409);
      sync = { ...sync, applied: undefined };
      return json(sync);
    }
    if (pathname.startsWith("/api/")) return json({});
    const file = path.join(assets, pathname === "/" ? "index.html" : pathname);
    const contentType = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".svg": "image/svg+xml", ".png": "image/png" }[path.extname(file)];
    return route.fulfill({ body: await fs.readFile(file), contentType });
  };
}

async function noneWider(page, sel) {
  const wide = await page.evaluate((sel) => {
    const out = [];
    if (document.scrollingElement.scrollWidth > innerWidth) out.push("page " + document.scrollingElement.scrollWidth);
    for (const r of document.querySelectorAll(sel)) {
      const b = r.getBoundingClientRect();
      if (b.right > innerWidth + 0.5) out.push((r.querySelector(".name")?.textContent || r.className) + " " + b.right);
    }
    return out;
  }, sel);
  assert.deepEqual(wide, [], "wider than the window");
}

for (const engine of (process.env.BROWSER ? [process.env.BROWSER] : ["chromium", "webkit"])) {
  for (const width of [1100, 420]) {
    for (const lang of ["en", "zh", "zh-TW", "ja", "de"]) {
      const w = (k, vars) => tr(lang, k, vars);
      test(`${engine} ${width}px ${lang}: pick what syncs, preview a sync, undo the last`, async (t) => {
        const browser = await (engine === "webkit" ? webkit.launch() : chromium.launch({ channel: "chromium" }));
        const page = await (await browser.newContext({ viewport: { width, height: 800 }, reducedMotion: "reduce" })).newPage();
        const errors = [], posts = [];
        page.on("pageerror", (e) => errors.push(e.message));
        let releaseRemote;
        const remoteGate = new Promise(resolve => {releaseRemote=resolve;});
        t.after(releaseRemote);
        await page.route("**/*", serve(lang, posts,remoteGate));
        t.after(async () => {
          if (process.env.ARTIFACT_DIR) {
            await fs.mkdir(process.env.ARTIFACT_DIR, { recursive: true });
            await page.screenshot({ path: path.join(process.env.ARTIFACT_DIR, `${engine}-${width}-${lang}-sync-scope.png`), fullPage: true });
          }
          await browser.close();
        });
        page.setDefaultTimeout(5000);
        page.setDefaultNavigationTimeout(60000);
        const all = (p) => posts.filter((x) => x.path === p);
        const waitPosts = async (p, n) => {
          for (let i = 0; i < 250 && all(p).length < n; i++) await page.waitForTimeout(20);
          assert.equal(all(p).length, n, `${p} posted ${n} times`);
          return all(p).at(-1).body;
        };
        const click = async (loc) => { await reach(page, loc); await loc.click(); };
        const exact = (s) => new RegExp("^" + s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "$");
        const parts = (ps) => ps.map((p) => w({ providers: "providers", settings: "settings", library: "library" }[p])).join(w(", "));

        await page.goto("http://magpie.test/?view=settings&tab=sync");
        const list = page.locator("#syncList");
        const first = list.locator(".row.pref").first();
        await first.waitFor({ state: "visible" });
        assert.equal(await list.locator(".sync-left").count(), 0, "a left-out line with every part synced");

        // the form: what syncs up front, the rare ones folded
        await click(first.locator("button", { hasText: exact(w("Edit")) }));
        const form = list.locator(".sync-form");
        const tickOf = (k) => form.locator("label.tick").filter({ hasText: exact(w(k)) });
        const prov = tickOf("Providers"), keys = tickOf("With their API keys"), sets = tickOf("Settings");
        for (const k of ["Providers", "With their API keys", "Settings", "Agents' models"]) {
          assert.equal(await tickOf(k).isVisible(), true, `${k} up front`);
          assert.equal(await tickOf(k).locator("input").isChecked(), true, `${k} on for a setup from before the choice`);
        }
        const adv = form.locator("details.sync-advanced");
        assert.equal(await adv.evaluate((d) => d.open), false, "Advanced open with nothing in it set");
        assert.equal(await tickOf("Profiles").isVisible(), false, "Profiles up front");
        await click(prov.locator("input"));
        assert.equal(await keys.isVisible(), false, "the keys tick with providers off");
        await click(prov.locator("input"));
        assert.equal(await keys.isVisible(), true, "the keys tick gone with providers on");
        await click(prov.locator("input"));
        await click(sets.locator("input"));
        await click(adv.locator("summary"));
        const sessionSync = tickOf("Sync readable sessions").locator("input");
        assert.equal(await sessionSync.isChecked(), false);
        await click(sessionSync);
        await noneWider(page, "#syncList .row.pref, #syncList .sync-form, #syncList .sync-form .bar > button");
        await click(form.locator("button", { hasText: exact(w("Save")) }));
        const saved = await waitPosts("/api/davsync/save", 1);
        assert.equal(saved.sessions, true);
        assert.equal(saved.providers, false);
        assert.equal(saved.settings, false);
        assert.equal("profiles" in saved, false, "profiles said with it left as it was");
        await form.waitFor({ state: "detached" });
        const left = list.locator(".sync-left");
        assert.equal(await left.textContent(), w("Stays as it is on each computer: {parts}", { parts: parts(["providers", "settings"]) }));

        // the preview: by part and by item, then Sync now with its token
        const peekRow = list.locator(".sync-preview-row");
        assert.equal(await peekRow.locator(".name").textContent(), w("Preview a sync"));
        await click(peekRow.locator("button", { hasText: exact(w("Preview…")) }));
        const modal = page.locator("#modal .sync-preview");
        await modal.waitFor({ state: "visible" });
        const text = await modal.textContent();
        for (const s of [
          w("Comes here from the server: {part}", { part: parts(["library"]) }),
          "review", "github", w("and {n} more", { n: 3 }),
          w("Memory: {n} come here", { n: 2 }), w("Memory: {n} go to the server", { n: 1 }),
          w("Stays as it is on each computer: {parts}", { parts: parts(["providers", "settings"]) }),
        ]) assert.ok(text.includes(s), `the preview says ${JSON.stringify(s)}: ${text}`);
        assert.equal(await modal.locator(".sync-item").count(), 8, "items past 8 counted, not listed");
        await noneWider(page, "#modal .sync-preview");
        await click(modal.locator("button", { hasText: exact(w("Sync now")) }));
        assert.equal((await waitPosts("/api/davsync/sync-previewed", 1)).token, "t1");
        // stale: shown again as it is now, with why
        await waitPosts("/api/davsync/preview", 2);
        await modal.locator(".editor-error", { hasText: w("something changed since the preview: look at it again before syncing") }).waitFor();
        await click(modal.locator("button", { hasText: exact(w("Sync now")) }));
        assert.equal((await waitPosts("/api/davsync/sync-previewed", 2)).token, "t2");
        await modal.waitFor({ state: "detached" });

        // undo: asked first, then again over a change made here since
        const undoRow = list.locator(".sync-undo-row");
        assert.equal(await undoRow.locator(".name").textContent(), w("Undo the last sync"));
        await click(undoRow.locator("button", { hasText: exact(w("Undo…")) }));
        const ask = page.locator("#modal .sync-undo-ask");
        await ask.waitFor({ state: "visible" });
        assert.ok((await ask.textContent()).includes(w("What is here now is kept in the sync folder first.")));
        await click(ask.locator("button", { hasText: exact(w("Undo sync")) }));
        assert.equal((await waitPosts("/api/davsync/undo-sync", 1)).force, false);
        await ask.locator(".editor-error", { hasText: w("{parts} changed here since the sync. Undoing replaces those changes too.", { parts: parts(["library"]) }) }).waitFor();
        await noneWider(page, "#modal .sync-undo-ask");
        await click(ask.locator("button", { hasText: exact(w("Undo anyway")) }));
        assert.equal((await waitPosts("/api/davsync/undo-sync", 2)).force, true);
        await ask.waitFor({ state: "detached" });
        await undoRow.waitFor({ state: "detached" });
        assert.equal(await list.locator(".sync-preview-row").count(), 1, "Preview a sync gone after the undo");
        const batch = list.locator(".sync-session-row", { hasText: w("Session sync") });
        const syncSessions = batch.getByRole("button", { name: w("Sync sessions now"), exact: true });
        await click(syncSessions);
        const confirmation = page.getByRole("alertdialog");
        await confirmation.getByRole("button", { name: w("Cancel"), exact: true }).click();
        await confirmation.waitFor({ state: "detached" });
        assert.equal(all("/api/davsync/session-sync-start").length, 0, "cancel did not start a sync");
        await click(batch.getByRole("button",{name:w("Open"),exact:true}));
        const panel = list.locator(".session-batch-form");
        const sessions = panel.locator(".session-batch-sources .session-batch-choice");
        const allSessions = panel.getByRole("checkbox", { name: w("All readable sessions") });
        assert.ok(await allSessions.isChecked(), "first open selects all sessions");
        await allSessions.uncheck();
        await sessions.nth(1).waitFor();
        const search = panel.getByRole("textbox", { name: w("Filter sessions") });
        await search.fill("another");
        assert.equal(await sessions.filter({ visible: true }).count(), 1);
        await search.clear();
        await panel.getByRole("button", { name: w("Source agent") }).click();
        await page.getByRole("menuitemradio", { name: "Codex", exact: true }).click();
        assert.equal(await sessions.filter({ visible: true }).count(), 1);
        assert.match(await sessions.filter({ visible: true }).textContent(), /Long title/);
        const geometry = await panel.evaluate((form) => {
          const list = form.querySelector(".session-batch-sources");
          const destinations = form.querySelector(".session-batch-targets");
          const backup = form.querySelector("textarea");
          const err = document.querySelector(".session-batch-error");
          const box = (node) => node.getBoundingClientRect();
          return {
            alignedFields: Math.abs(box(list).left - box(backup).left) < 1 && Math.abs(box(list).width - box(backup).width) < 1,
            separateRows: box(backup).top >= box(list).bottom && box(destinations).top >= box(backup).bottom,
            listFits: list.scrollWidth <= list.clientWidth + 1,
            errorFits: !!err.closest(".sync-session-row") && err.scrollWidth <= err.clientWidth + 1,
          };
        });
        assert.deepEqual(geometry, { alignedFields: true, separateRows: true, listFits: true, errorFits: true });
        await noneWider(page, ".session-batch-form, .session-batch-form .bar > button");
        if (process.env.ARTIFACT_DIR) {
          await fs.mkdir(process.env.ARTIFACT_DIR, { recursive: true });
          await panel.screenshot({ path: path.join(process.env.ARTIFACT_DIR, `${engine}-${width}-${lang}-session-batch.png`) });
        }
        await click(syncSessions);
        const confirm = page.getByRole("alertdialog").getByRole("button",{name:w("Sync sessions now"),exact:true});
        await confirm.click();
        await batch.getByRole("button",{name:w("Retry now"),exact:true}).waitFor();
        await click(batch.getByRole("button", {name:w("Close"),exact:true}));
        await page.goto("http://magpie.test/?view=settings&tab=sync");
        await batch.waitFor({state:"visible"});
        await batch.getByRole("button", {name:w("Retry now"),exact:true}).waitFor();
        assert.equal(all("/api/davsync/session-sync-start").length, 1, "reopening attached to the running job");
        await click(batch.getByRole("button", {name:w("Retry now"),exact:true}));
        await click(batch.getByRole("button",{name:w("Open"),exact:true}));
        await batch.getByText(w("Remote files match packed size · skipped {skipped}",{skipped:2}),{exact:true}).waitFor();
        assert.equal(all("/api/davsync/session-sync-start").length,2);

        await sessions.filter({ visible: true }).getByRole("checkbox").check();
        await search.fill("Long title");
        await panel.getByRole("textbox", { name: w("Existing backup IDs") }).fill("example");
        await panel.getByRole("group", { name: w("Import into agents") }).getByRole("checkbox", { name: "Codex" }).check();
        const autoImport = panel.getByRole("checkbox",{name:w("Import remote sessions into selected agents on WebDAV sync")});
        assert.equal(await autoImport.isChecked(), false);
        await autoImport.check();
        const savedDraft = page.waitForResponse(response => response.status() === 204 &&
          response.url().endsWith("/api/davsync/session-preferences") &&
          response.request().postDataJSON()?.cwd === "/fixture/project");
        assert.equal(await panel.getByRole("textbox", {name:w("Target project folder")}).count(),0);
        await click(panel.locator(".session-batch-advanced > summary"));
        await panel.getByRole("textbox", { name: w("Target project folder") }).fill("/fixture/project");
        await savedDraft;
        await page.goto("http://magpie.test/?view=settings&tab=sync");
        await batch.waitFor({state:"visible"});
        await click(batch.getByRole("button",{name:w("Open"),exact:true}));
        await panel.getByRole("textbox", { name: w("Target project folder") }).waitFor();
        await page.waitForFunction(() =>
          [...document.querySelectorAll(".session-batch-form input")].some(input => input.value === "/fixture/project"));
        assert.equal(await allSessions.isChecked(), false);
        assert.equal(await search.inputValue(), "Long title");
        assert.ok(await autoImport.isChecked(), "automatic import survives reopening Settings");
        assert.equal(await panel.getByRole("button", { name: w("Source agent") }).textContent(), "Codex");
        assert.equal(await sessions.filter({ visible: true }).count(), 1);
        assert.ok(await sessions.filter({ visible: true }).getByRole("checkbox").isChecked());
        assert.equal(await panel.getByRole("textbox", { name: w("Existing backup IDs") }).inputValue(), "example");
        assert.ok(await panel.getByRole("group", { name: w("Import into agents") }).getByRole("checkbox", { name: "Codex" }).isChecked());
        assert.equal(all("/api/davsync/session-sync-start").length, 2, "editing a draft did not start a transfer");
        const manager=panel.locator(".session-batch-manager");
        await click(manager.locator("summary").first());
        await manager.getByText("Laptop · Restore fixture").waitFor();
        assert.equal(all("/api/davsync/session-restore-choices").filter(x=>x.body.verify).length,0);
        await click(manager.getByRole("button",{name:w("View backup"),exact:true}));
        await click(manager.getByText(w("Choose a conversation"),{exact:true}));
        await click(manager.getByRole("button",{name:"Restore fixture",exact:true}));
        await click(panel.getByRole("button",{name:w("Import"),exact:true}));
        releaseRemote(); // the local restore was usable throughout a stalled remote listing
        const restore = await waitPosts("/api/davsync/session-batch",1);
        assert.deepEqual(restore.backups,["snapshot"]);
        assert.equal(restore.entry,"thread");
        assert.equal(restore.all,false);
        assert.deepEqual(restore.sources,[]);
        assert.deepEqual(errors, []);
      });
    }
  }
}
