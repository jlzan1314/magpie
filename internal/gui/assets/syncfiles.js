// Settings › Sync's forms: the server's (WebDAV, S3 or GitHub), Export and
// Import, what putting a file's sign-ins in came to (qianxunji,
// curtinp118 on Discord), and the asks before a Restore or an Upload
// (#847, #5720). app.js imports this when one of them opens (syncFile), so
// it weighs nothing on the core.

// davForm: the kind picked shows one set of fields; drafts survive switching.
// Inactive servers keep their settings for moving sync back.
window.davForm = function (v) {
  const ed = el("div", "editor sync-form");
  const active = v.on ? (v.kind || "webdav") : "";
  let kind = active || "webdav";
  const saved = t("saved · type a new one to replace it");
  const servers = syncServers(v);
  const server = (k) => active === k ? v : servers[k] || {};
  const dav = server("webdav"), bk = server("s3"), gh = server("github");
  const url = input(dav.url || "", "https://dav.jianguoyun.com/dav/");
  const user = input(dav.user || "", t("user name"));
  const pass = input("", dav.passwordSet ? saved : t("password, or an app password"), "password");
  // s3://bucket/prefix, as the address is kept
  const [bucketName, prefixName] = (() => { const m = /^s3:\/\/([^/]*)\/?(.*)$/i.exec(bk.url || ""); return m ? [m[1], m[2]] : ["", ""]; })();
  const endpoint = input(bk.endpoint || "", "https://<account>.r2.cloudflarestorage.com");
  const bucket = input(bucketName, t("bucket name"));
  const prefix = input(prefixName, t("optional"));
  const region = input(bk.region || "", t("us-east-1, or auto for R2"));
  const keyID = input(bk.user || "", "AKIA…");
  const secret = input("", bk.passwordSet ? saved : t("secret access key"), "password");
  const [pathL, pathStyle] = tick(t("Path-style: the bucket in the path, not the host name"), !!bk.pathStyle);
  const ghParts = /^github:\/\/([^/]+\/[^/]+)\/?(.*)$/i.exec(gh.url || "");
  const repo = input(ghParts?.[1] || "", "owner/repo");
  const branch = input(gh.branch || "", t("default branch"));
  const ghPrefix = input(ghParts?.[2] || "", t("optional"));
  const token = input("", gh.passwordSet ? saved : t("GitHub token"), "password");
  const phrase = input("", v.passphraseSet ? saved : t("the same on every computer"), "password");
  // what syncs (#6428, jinpwng on X): a part not ticked stays as it is on
  // each computer, and the server keeps the others' copy of it
  const [provL, prov] = tick(t("Providers"), v.providers !== false);
  const [keysL, keys] = tick(t("With their API keys"), v.keys !== false);
  keysL.classList.add("sync-sub-tick");
  const [setL, sets] = tick(t("Settings"), v.settings !== false);
  const [agentsL, agents] = tick(t("Agents' models"), v.agents !== false);
  const [libL, lib] = tick(t("Library: instructions, MCP servers, skills, hooks, subagents and Claude plugins"), v.library !== false);
  const [memL, mem] = tick(t("Memory: what the agents know, merged memory by memory"), v.memory !== false);
  const keysShow = () => { keysL.hidden = !prov.checked; };
  keysShow();
  prov.onchange = keysShow;
  // rarely changed: under Advanced, open when one is set away from its default
  const [profL, prof] = tick(t("Profiles"), v.profiles !== false);
  // usage: off unless picked, each computer its own (#542)
  const [useL, use] = tick(t("Usage: this computer's calls, for the Usage page on the others"), !!v.usage);
  const [sessL, sess] = tick(t("Sessions: sync local session changes, skipping files with matching remote size"), !!v.sessions);
  // sign-ins: off unless picked, sealed with the passphrase like the rest
  // (qianxunji, curtinp118 on Discord); what it lets in is said beside it
  const [siL, si] = tick(t("Sign-ins: subscription accounts and plugin sign-ins"), !!v.signIns);
  const siSay = el("div", "hint sync-signins-say", t("Another computer that syncs is signed in without signing in again. Anyone with the passphrase and the server's file can use these accounts. An account used on two computers can be signed out on one when the other renews it."));
  // subscriptions whose sign-ins stay on this computer (Ethereal499 on X:
  // Claude, wary of where it is used): theirs here never go up, the
  // server's never come in, and none here is taken away
  let except = [...(v.signInsExcept || [])];
  const exL = el("div", "sync-sub-tick sync-except");
  const exPick = el("button", "proto pick");
  exPick.type = "button";
  const subName = (id) => providers?.providers?.find((p) => p.id === id)?.name || id;
  const exPaint = () => {
    exPick.classList.toggle("set", !!except.length);
    exPick.replaceChildren(el("span", "", !except.length ? t("None") : except.length > 2 ? t("{n} subscriptions", { n: except.length }) : except.map(subName).join(", ")), svg(CHEV, 11, 1.6));
  };
  exPaint();
  if (except.length && !providers) loadProviders().catch(() => {}).then(exPaint);
  exPick.onclick = async (e) => {
    e.stopPropagation();
    if (exPick.classList.contains("open")) return closeProtoMenu();
    if (!providers) { exPick.classList.add("busy"); await loadProviders().catch(() => {}); exPick.classList.remove("busy"); if (!exPick.isConnected) return; }
    const opts = (providers?.providers || []).filter((p) => p.account).map((p) => ({ v: p.id, name: p.name || p.id, note: "", literalName: true }));
    // one left out that isn't here now stays to be ticked back
    for (const id of except) if (!opts.some((o) => o.v === id)) opts.push({ v: id, name: id, note: "", literalName: true });
    if (!opts.length) opts.push({ v: "\x00", name: "No subscriptions yet", note: "" });
    openProtoMenu(exPick, opts, except, (ids) => { except = ids; exPaint(); }, "Not synced", "", "left", true);
  };
  exL.append(el("span", "", t("Not synced")), exPick);
  const siShow = () => { siSay.hidden = exL.hidden = !si.checked; };
  siShow();
  si.onchange = siShow;
  const what = el("div", "stack");
  what.append(provL, keysL, setL, agentsL, libL, memL);
  const adv = el("details", "more advanced sync-advanced");
  adv.open = v.profiles === false || !!v.usage || !!v.sessions || !!v.signIns;
  const advIn = el("div", "stack");
  advIn.append(profL, useL, sessL, siL, exL, siSay, el("div", "hint", t("Usage goes as a file a day: times, tokens, models and costs, never what was said. Turn it on on each computer that shares its own.")));
  adv.append(el("summary", "", t("Advanced")), advIn);
  const davFields = [...field(t("Address"), url, t("A folder named magpie is made in it.")),
    ...field(t("User"), user),
    ...field(t("Password"), pass)];
  const s3Fields = [...field(t("Endpoint"), endpoint, t("Empty for AWS S3; for R2, B2, MinIO and the like, their S3 API address.")),
    ...field(t("Bucket"), bucket),
    ...field(t("Prefix"), prefix, t("The backup goes in a folder named magpie under it.")),
    ...field(t("Region"), region),
    ...field(t("Access key"), keyID),
    ...field(t("Secret"), secret),
    ...field("", pathL, t("MinIO and most NAS servers need it."))];
  const ghFields = [...field(t("Repository"), repo, t("An existing private GitHub repository, in owner/repo form.")),
    ...field(t("Branch"), branch, t("Empty uses the repository's default branch. Empty repositories are initialized by the first sync; other branches must already exist.")),
    ...field(t("Folder"), ghPrefix, t("The encrypted backup goes in <folder>/magpie/magpie.magpie-backup. Each update creates a commit.")),
    ...field(t("GitHub token"), token, t("A fine-grained token for this repository with Contents: read and write. Kept on this computer, separately from the library's GitHub token."))];
  // which one is synced to, and that the other's settings stay
  const to = field(t("Sync to"), segs(Object.entries(syncNames).map(([id, n]) => [id, id === active ? t("{kind} · on", { kind: n }) : n]), kind, (k) => { kind = k; show(); }), " ");
  const where = to[1].querySelector(".hint");
  const save = el("button", "text primary", "");
  const other = Object.keys(servers).map((k) => syncNames[k]).filter(Boolean).join(" / ");
  const show = () => {
    for (const x of davFields) x.hidden = kind !== "webdav";
    for (const x of s3Fields) x.hidden = kind !== "s3";
    for (const x of ghFields) x.hidden = kind !== "github";
    sessL.hidden = kind !== "webdav";
    where.textContent = !active ? ""
      : kind === active ? (other ? t("Syncing here now. The {other} settings are kept, not synced to: pick {other} to see them.", { other }) : t("Syncing here now."))
      : t("{active} is synced to now. Saving moves sync here; the {active} settings are kept for moving back.", { active: syncNames[active] });
    where.hidden = !active;
    save.textContent = !active ? t("Turn on") : kind === active ? t("Save") : t("Move sync to {kind}", { kind: syncNames[kind] });
  };
  ed.append(...to,
    ...davFields, ...s3Fields, ...ghFields,
    ...field(t("Passphrase"), phrase, t("The file is sealed with it on this computer; the server only ever sees it sealed. Keep it: without it the file can't be opened.")),
    ...field(t("What syncs"), what, t("What isn't ticked stays as it is on each computer.")),
    ...field("", adv));
  show();
  const off = v.on ? el("button", "text danger", t("Turn off")) : el("span");
  const cancel = el("button", "text", t("Cancel"));
  // this setup as text, for another computer (WJaunita45604 on X): sealed
  // with the passphrase, which the text never holds
  const copySetup = v.on ? el("button", "text sync-copy", t("Copy setup")) : el("span");
  // what saving would sync, before it does (#6428)
  const peek = el("button", "text sync-peek", t("Preview"));
  const said = syncBar(ed, "", off, copySetup, el("span", "grow"), cancel, peek, save);
  const say = (msg, note) => { said(msg); ed.querySelector(".editor-error").classList.toggle("note", !!note); };
  copySetup.onclick = async () => {
    try {
      const r = await api("davsync/setup-text", {});
      await copy(r.text, t("Sync setup"), copySetup, t("Sync setup copied: the server and its password, sealed with your passphrase. On the other computer, paste it into this form and type the same passphrase."));
    } catch (e) { say(t(e.message)); }
  };
  // a setup copied on another computer, pasted in any field here, fills
  // the form once the passphrase it was sealed with is typed
  let pasted = "";
  const fill = (s) => {
    kind = /^s3:\/\//i.test(s.url) ? "s3" : /^github:\/\//i.test(s.url) ? "github" : "webdav";
    if (kind === "s3") {
      const m = /^s3:\/\/([^/]*)\/?(.*)$/i.exec(s.url);
      bucket.value = m[1]; prefix.value = m[2];
      endpoint.value = s.endpoint || ""; region.value = s.region || ""; keyID.value = s.user || ""; secret.value = s.password || ""; pathStyle.checked = !!s.pathStyle;
    } else if (kind === "github") {
      const m = /^github:\/\/([^/]+\/[^/]+)\/?(.*)$/i.exec(s.url);
      repo.value = m?.[1] || ""; ghPrefix.value = m?.[2] || ""; branch.value = s.branch || ""; token.value = s.password || "";
    } else { url.value = s.url; user.value = s.user || ""; pass.value = s.password || ""; }
    keys.checked = !!s.keys; agents.checked = !!s.agents; lib.checked = !!s.library; mem.checked = !!s.memory; use.checked = !!s.usage; sess.checked = !!s.sessions; si.checked = !!s.signIns;
    // left out only when the setup says so: one from before the choice syncs them
    prov.checked = s.providers !== false; sets.checked = s.settings !== false; prof.checked = s.profiles !== false;
    keysShow();
    if (!prof.checked || use.checked || sess.checked || si.checked) adv.open = true;
    siShow();
    // the kind picked as a click on it would, its thumb moved too
    to[1].querySelector(`.segs [data-seg="${kind}"]`)?.click();
    show();
  };
  const openPasted = async () => {
    if (!pasted) return true;
    if (!phrase.value) {
      say(t("Type the passphrase sync uses on the other computer: the pasted setup is sealed with it"), true);
      phrase.focus();
      return false;
    }
    try {
      fill(await api("davsync/setup-open", { text: pasted, passphrase: phrase.value }));
      pasted = "";
      say(t("Filled in from the pasted setup."), true);
      return true;
    } catch (e) { say(t(e.message)); return false; }
  };
  ed.addEventListener("paste", (e) => {
    const text = e.clipboardData?.getData("text") || "";
    if (!text.includes("magpie-sync:")) return;
    e.preventDefault();
    pasted = text;
    openPasted();
  });
  phrase.addEventListener("keydown", (e) => { if (e.key === "Enter" && pasted) openPasted(); });
  phrase.addEventListener("change", () => { if (pasted) openPasted(); });
  cancel.onclick = () => { syncOpen = ""; renderSync(); };
  off.onclick = async () => { syncOpen = ""; renderSync(await api("davsync/off", {}).catch(() => null) || undefined); };
  // form: the setup as typed, or a reason it can't be saved yet
  const form = () => {
    const b = bucket.value.trim(), p = prefix.value.trim().replace(/^\/+|\/+$/g, "");
    if (kind === "s3" && !b) return t("Name the bucket");
    const repository = repo.value.trim().replace(/^https:\/\/github\.com\//i, "").replace(/\/+$/, "").replace(/\.git$/i, "");
    const ghFolder = ghPrefix.value.trim().replace(/^\/+|\/+$/g, "");
    if (kind === "github" && !/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repository)) return t("Name the GitHub repository as owner/repo");
    if (kind === "github" && !gh.passwordSet && !token.value.trim()) return t("Enter a GitHub token with Contents read and write permission");
    if (!v.passphraseSet && !phrase.value) return t("Pick a passphrase: the file is sealed with it");
    const where = kind === "github"
      ? { url: "github://" + repository + (ghFolder ? "/" + ghFolder : ""), branch: branch.value.trim(), password: token.value.trim() }
      : kind === "s3"
      ? { url: "s3://" + b + (p ? "/" + p : ""), user: keyID.value.trim(), password: secret.value, endpoint: endpoint.value.trim(), region: region.value.trim(), pathStyle: pathStyle.checked }
      : { url: url.value.trim(), user: user.value.trim(), password: pass.value };
    // a part left out is said; one synced only when it was left out before,
    // so a setup from before the choice is saved as it was
    const outs = {};
    for (const [k, c] of [["providers", prov], ["settings", sets], ["profiles", prof]]) if (!c.checked || v[k] === false) outs[k] = c.checked;
    return { ...where, passphrase: phrase.value, keys: keys.checked, agents: agents.checked, library: lib.checked, memory: mem.checked, ...outs, ...(use.checked || v.usage ? { usage: use.checked } : {}), ...(kind === "webdav" && (sess.checked || v.sessions) ? { sessions: sess.checked } : {}), ...(si.checked || v.signIns ? { signIns: si.checked } : {}), ...(except.length || v.signInsExcept?.length ? { signInsExcept: except } : {}) };
  };
  peek.onclick = async () => {
    if (!(await openPasted())) return;
    const f = form();
    if (typeof f === "string") return say(f);
    peek.classList.add("busy");
    try { askSyncPreview(await api("davsync/preview", { form: f }), () => save.click()); } catch (e) { say(t(e.message)); }
    peek.classList.remove("busy");
  };
  save.onclick = async () => {
    if (!(await openPasted())) return;
    const f = form();
    if (typeof f === "string") return say(f);
    save.classList.add("busy");
    try {
      const r = await api("davsync/save", f);
      if (!r.error) syncOpen = "";
      renderSync(r);
      if (r.error) return;
      refreshAfterSync();
    } catch (e) {
      save.classList.remove("busy");
      say(e.message);
    }
  };
  return ed;
};

window.exportForm = function () {
  const ed = el("div", "editor sync-form");
  const p1 = input("", t("passphrase"), "password");
  const p2 = input("", t("again"), "password");
  const [keysL, keys] = tick(t("With the providers' API keys"), true);
  const [libL, lib] = tick(t("With the library: instructions, MCP servers, skills, hooks, subagents and Claude plugins"), true);
  const [memL, mem] = tick(t("With memory: what the agents know"), true);
  const [useL, use] = tick(t("With the usage history"), true);
  // sign-ins: off unless picked (qianxunji, curtinp118 on Discord)
  const [siL, si] = tick(t("With the sign-ins: subscription accounts and plugin sign-ins"), false);
  const what = el("div", "stack");
  what.append(keysL, libL, memL, useL, siL);
  const passF = field(t("Passphrase"), p1, t("Needed to open the file. Subscriptions aren't in it: sign in to them on the other computer."));
  const passSay = passF[1].querySelector(".hint");
  si.onchange = () => {
    passSay.textContent = si.checked ? t("Needed to open the file. The sign-ins are in it: anyone with the file and the passphrase can use those accounts, so pick a strong one.")
      : t("Needed to open the file. Subscriptions aren't in it: sign in to them on the other computer.");
  };
  ed.append(...passF, ...field("", p2), ...field("", what));
  const go = el("button", "text primary", t("Export"));
  const cancel = el("button", "text", t("Cancel"));
  const say = syncBar(ed, "", el("span", "grow"), cancel, go);
  cancel.onclick = () => { syncOpen = ""; renderSync(); };
  go.onclick = async () => {
    if (!p1.value) return say(t("Pick a passphrase: the file is sealed with it"));
    if (p1.value !== p2.value) return say(t("The two passphrases differ"));
    go.classList.add("busy");
    try {
      const r = await api("backup/export", { pass: p1.value, keys: keys.checked, library: lib.checked, memory: mem.checked, usage: use.checked, signIns: si.checked });
      ed.replaceChildren(el("div", "done", t("Saved to {path}", { path: r.path })));
    } catch (e) {
      go.classList.remove("busy");
      say(e.message);
    }
  };
  return ed;
};

// signInsSaid: what putting a backup's sign-ins in did, as lines
window.signInsSaid = function (r) {
  const lines = [t("Sign-ins: {added} added, {updated} updated, {kept} kept as they are here", { added: r.added || 0, updated: r.updated || 0, kept: r.kept || 0 })];
  if (r.kept) lines.push(t("One kept as it is here was renewed later here, or is the account the agent itself is signed in with."));
  if (r.codex) lines.push(t("Codex is signed in with the backup's account."));
  return lines;
};

window.importForm = function () {
  const ed = el("div", "editor sync-form");
  let data = "";
  const file = el("input");
  file.type = "file";
  file.accept = ".magpie-backup";
  file.hidden = true;
  const name = el("span", "fname", t("No file chosen"));
  const pick = el("button", "text", t("Choose…"));
  pick.onclick = () => file.click();
  file.onchange = async () => {
    const f = file.files[0];
    if (!f) return;
    // as base64 in JSON: the app's web view drops a File sent as the body
    const bytes = new Uint8Array(await f.arrayBuffer());
    let bin = "";
    for (let i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
    data = btoa(bin);
    name.textContent = f.name;
  };
  const pf = el("div", "pair");
  pf.append(name, pick, file);
  const pass = input("", t("passphrase"), "password");
  const [agentsL, agents] = tick(t("Set the agents' models too"), true);
  const [libL, lib] = tick(t("Bring in the library too: instructions, MCP servers, skills, hooks, subagents and Claude plugins"), true);
  const [memL, mem] = tick(t("Bring in memory too, merged with the memories here"), true);
  const [useL, use] = tick(t("Bring in the usage history too, merged with the one here"), true);
  const [siL, si] = tick(t("Bring in the sign-ins too, when the file has them"), true);
  const what = el("div", "stack");
  what.append(agentsL, libL, memL, useL, siL);
  ed.append(...field(t("File"), pf), ...field(t("Passphrase"), pass), ...field("", what, t("Providers with the same id are replaced; one that came without a key keeps the key it has here. The library replaces the one here, which is kept with its backups.")));
  const go = el("button", "text primary", t("Import"));
  const cancel = el("button", "text", t("Cancel"));
  const say = syncBar(ed, "", el("span", "grow"), cancel, go);
  cancel.onclick = () => { syncOpen = ""; renderSync(); };
  go.onclick = async () => {
    if (!data) return say(t("Choose a file first"));
    go.classList.add("busy");
    try {
      const r = await api("backup/import", { data, pass: pass.value, agents: agents.checked, library: lib.checked, memory: mem.checked, usage: use.checked, signIns: si.checked });
      const lines = [t("Providers: {added} added, {replaced} replaced; {profiles} profiles; {agents} agent settings changed", { added: r.Added, replaced: r.Replaced, profiles: r.Profiles, agents: r.Agents })];
      if (r.NeedKey?.length) lines.push(t("Needs a key: {names}", { names: r.NeedKey.join(", ") }));
      if (r.SignIns) lines.push(...signInsSaid(r.SignIns));
      // a backup carries a subscription's settings, its sign-in only when
      // made with them
      if (r.SignIn?.length) lines.push(r.SignIns
        ? t("This backup's settings use {names}, which isn't signed in on this computer, and the backup has no sign-in for it. To use it here, sign in in Providers › Add a provider; if you don't, nothing needs doing.", { names: subNames(r.SignIn) })
        : t("This backup's settings use {names}, which isn't signed in on this computer. A backup carries settings, not sign-ins: to use it here, sign in in Providers › Add a provider; if you don't, nothing needs doing.", { names: subNames(r.SignIn) }));
      if (r.Plugins?.length) lines.push(t("Plugins added: {names}", { names: r.Plugins.join(", ") }));
      for (const [spec, error] of Object.entries(r.PluginErrors || {})) lines.push(t("Couldn't add the plugin {spec}: {error}", { spec, error }));
      if (r.Library) lines.push(t("Library brought in and written into the agents"));
      if (r.Usage != null) lines.push(t("Usage history: {n} calls added", { n: r.Usage }));
      for (const p of r.LibraryProblems || []) lines.push(t("{agent} couldn't get {what}: {error}", { agent: p.agent, what: p.what, error: p.error }));
      ed.replaceChildren(...lines.map((l) => el("div", "done", l)));
      refreshAfterSync();
    } catch (e) {
      go.classList.remove("busy");
      say(e.message);
    }
  };
  return ed;
};

// askRestore asks before the server's setup replaces this computer's
// (#847); what is here is kept, and the notice it leaves can undo it.
window.askRestore = function (v, parts) {
  const ed = el("div", "editor restore-ask");
  const head = el("div", "ehead");
  head.append(el("b", "", t("Restore from the server?")));
  ed.append(head);
  const goes = ["providers", "settings", "profiles", "agents", "library"].filter((p) => v[p] !== false);
  ed.append(el("p", "lib-confirm", t("This computer's {parts} become the ones on the server, as your other computers last synced them. Whatever was changed here since is replaced.", { parts: parts(goes) })));
  ed.append(el("p", "lib-confirm", t("What is here now is sealed into the sync folder first, and Undo puts it back.")));
  const err = el("p", "editor-error", "");
  ed.append(err);
  const bar = el("div", "bar");
  const go = el("button", "text primary danger-fill", t("Restore setup"));
  go.onclick = async (e) => {
    e.stopPropagation();
    go.disabled = true;
    go.classList.add("busy");
    try {
      const r = await api("davsync/restore", {});
      closeConfirmAsk();
      renderSync(r);
      if (r.error) return;
      if (r.brought?.length) refreshAfterSync();
      else status(t("This computer's setup is already the server's: nothing to restore"), "ok");
    } catch (x) {
      go.disabled = false;
      go.classList.remove("busy");
      err.textContent = x.message;
    }
  };
  const cancel = el("button", "text", t("Cancel"));
  cancel.onclick = cancelAsk;
  bar.append(el("span", "grow"), cancel, go);
  ed.append(bar);
  confirmAsk = ed;
  openModal(ed);
  $("#modal").classList.add("lib");
  cancel.focus(NO_SCROLL);
};

// askUpload asks before this computer's setup replaces a server file that
// isn't a backup; the server's file is kept in the sync folder first.
window.askUpload = function (f) {
  const ed = el("div", "editor restore-ask upload-ask");
  const head = el("div", "ehead");
  head.append(el("b", "", t("Replace the file on the server?")));
  ed.append(head);
  ed.append(el("p", "lib-confirm", t("This computer's setup goes up in place of the file on the server, and your other computers sync with it from then on.")));
  if (f.what === "cut") ed.append(el("p", "lib-confirm", t("What another computer wrote into that file and has nowhere else is lost. If one of them synced it last, Sync now there may rebuild it instead.")));
  ed.append(el("p", "lib-confirm", t("The file on the server is kept in the sync folder first.")));
  const err = el("p", "editor-error", "");
  ed.append(err);
  const bar = el("div", "bar");
  const go = el("button", "text primary danger-fill", t("Upload this computer's setup"));
  go.onclick = async (e) => {
    e.stopPropagation();
    go.disabled = true;
    go.classList.add("busy");
    try {
      const r = await api("davsync/upload", {});
      closeConfirmAsk();
      renderSync(r);
    } catch (x) {
      go.disabled = false;
      go.classList.remove("busy");
      err.textContent = x.message;
    }
  };
  const cancel = el("button", "text", t("Cancel"));
  cancel.onclick = cancelAsk;
  bar.append(el("span", "grow"), cancel, go);
  ed.append(bar);
  confirmAsk = ed;
  openModal(ed);
  $("#modal").classList.add("lib");
  cancel.focus(NO_SCROLL);
};

// askSyncPreview shows what syncing now would do (#6428, jinpwng on X),
// nothing done yet: what comes here, what goes to the server, and what
// stays as it is. go is the form's save, when the preview is of a setup
// not saved yet; otherwise the sync shown is the one run, or none.
window.askSyncPreview = function (plan, go) {
  const ed = el("div", "editor restore-ask sync-preview");
  const draw = (plan, note) => {
    ed.replaceChildren();
    const head = el("div", "ehead");
    head.append(el("b", "", t("Preview of the sync")));
    ed.append(head);
    const by = (d) => plan.parts.filter((p) => p.do === d);
    const items = (p) => {
      const list = el("div", "sync-items");
      const shown = (p.items || []).slice(0, 8);
      for (const it of shown) {
        const r = el("div", "sync-item");
        r.append(el("span", "", it.name || t(it.kind)), el("span", "sub", t({ added: "added", removed: "removed", changed: "changed" }[it.change] || it.change)));
        list.append(r);
      }
      if ((p.items || []).length > shown.length) list.append(el("div", "sub", t("and {n} more", { n: p.items.length - shown.length })));
      return list;
    };
    const ins = by("in"), ups = by("up");
    const mem = plan.memory || { in: 0, up: 0 };
    if (plan.empty) ed.append(el("p", "lib-confirm", t("The server has no backup yet: this computer's setup goes up.")));
    for (const p of ins) {
      ed.append(el("p", "lib-confirm", t("Comes here from the server: {part}", { part: syncParts([p.part]) })));
      if (p.replaces) ed.append(el("p", "lib-confirm sub", t("It was changed here too: the server's newer one replaces it, and what is here is kept in the sync folder first.")));
      if (p.items?.length) ed.append(items(p));
    }
    for (const p of ups) {
      ed.append(el("p", "lib-confirm", t("Goes to the server from here: {part}", { part: syncParts([p.part]) })));
      if (!plan.empty && p.items?.length) ed.append(items(p));
    }
    if (mem.in) ed.append(el("p", "lib-confirm", t("Memory: {n} come here", { n: mem.in })));
    if (mem.up) ed.append(el("p", "lib-confirm", t("Memory: {n} go to the server", { n: mem.up })));
    if (plan.signIns) ed.append(el("p", "lib-confirm", t("Sign-ins: {n} change here", { n: plan.signIns })));
    if (plan.pluginsGone?.length) ed.append(el("p", "lib-confirm", t("Plugins removed here, as another computer removed them: {names}", { names: plan.pluginsGone.join(", ") })));
    const nothing = !ins.length && !ups.length && !mem.in && !mem.up && !plan.signIns && !plan.pluginsGone?.length;
    if (nothing) ed.append(el("p", "lib-confirm", t("Nothing to sync: this computer and the server are the same.")));
    const same = by("same").map((p) => p.part), off = by("off").map((p) => p.part);
    if (same.length && !nothing) ed.append(el("p", "lib-confirm sub", t("Already the same: {parts}", { parts: syncParts(same) })));
    if (off.length) ed.append(el("p", "lib-confirm sub", t("Stays as it is on each computer: {parts}", { parts: syncParts(off) })));
    const err = el("p", "editor-error", note || "");
    err.setAttribute("role", "alert");
    ed.append(err);
    const bar = el("div", "bar");
    const cancel = el("button", "text", t(nothing && !go ? "Close" : "Cancel"));
    cancel.onclick = cancelAsk;
    bar.append(el("span", "grow"), cancel);
    if (go || !nothing) {
      const ok = el("button", "text primary", t(go ? "Save and sync" : "Sync now"));
      ok.onclick = async (e) => {
        e.stopPropagation();
        if (go) { closeConfirmAsk(); go(); return; }
        ok.disabled = true;
        ok.classList.add("busy");
        try {
          const r = await api("davsync/sync-previewed", { token: plan.token });
          closeConfirmAsk();
          renderSync(r);
          if (!r.error) refreshAfterSync();
        } catch (x) {
          // the server or this computer changed since: shown again, as it is now
          try { draw(await api("davsync/preview", {}), t(x.message)); } catch (y) { ok.disabled = false; ok.classList.remove("busy"); err.textContent = t(y.message); }
        }
      };
      bar.append(ok);
    }
    ed.append(bar);
    cancel.focus(NO_SCROLL);
  };
  draw(plan);
  confirmAsk = ed;
  openModal(ed);
  $("#modal").classList.add("lib");
  ed.querySelector(".bar .text")?.focus(NO_SCROLL);
};

// askUndoSync asks before the parts the last sync brought in go back to
// how they were on this computer (#6428). A part changed here since is
// named, and written over only when asked again.
window.askUndoSync = function (v) {
  const ed = el("div", "editor restore-ask sync-undo-ask");
  const head = el("div", "ehead");
  head.append(el("b", "", t("Undo the last sync?")));
  ed.append(head);
  const a = v.applied;
  ed.append(el("p", "lib-confirm", t("This computer's {parts} go back to how they were before the sync {when}. The server and your other computers keep theirs.", { parts: syncParts(a.parts), when: syncWhen(a.at) })));
  ed.append(el("p", "lib-confirm", t("What is here now is kept in the sync folder first.")));
  const err = el("p", "editor-error", "");
  err.setAttribute("role", "alert");
  ed.append(err);
  const bar = el("div", "bar");
  const go = el("button", "text primary danger-fill", t("Undo sync"));
  let force = false;
  go.onclick = async (e) => {
    e.stopPropagation();
    go.disabled = true;
    go.classList.add("busy");
    try {
      const r = await api("davsync/undo-sync", { force });
      closeConfirmAsk();
      renderSync(r);
      refreshAfterSync();
    } catch (x) {
      go.disabled = false;
      go.classList.remove("busy");
      if (x.changed?.length) {
        force = true;
        err.classList.add("note");
        err.textContent = t("{parts} changed here since the sync. Undoing replaces those changes too.", { parts: syncParts(x.changed) });
        go.textContent = t("Undo anyway");
      } else err.textContent = t(x.message);
    }
  };
  const cancel = el("button", "text", t("Cancel"));
  cancel.onclick = cancelAsk;
  bar.append(el("span", "grow"), cancel, go);
  ed.append(bar);
  confirmAsk = ed;
  openModal(ed);
  $("#modal").classList.add("lib");
  cancel.focus(NO_SCROLL);
};

// askPreviewRows draws Settings › Sync's Preview a sync, and Undo the last
// sync while there is one to undo (#6428), into at: renderSync's place for
// them, kept here so the core weighs nothing more. Under sub, the sync's
// line, it says which parts each computer keeps as it is.
window.askPreviewRows = function (at, v, sub) {
  const left = ["providers", "settings", "profiles", "agents", "library", "memory"].filter((p) => v[p] === false);
  if (left.length) sub.after(el("div", "sub wraps sync-left", t("Stays as it is on each computer: {parts}", { parts: syncParts(left) })));
  // the library's files too big to sync, and skills a sync couldn't fetch
  // from their source here: said, never a silent success (#5715)
  const lib = [v.libraryLeft?.length && t("Files too big to sync stay on this computer only, in: {names}", { names: v.libraryLeft.join(t(", ")) }), ...v.libraryProblems || []].filter(Boolean);
  if (!v.error && lib.length) {
    const d = el("div", "sub bad wraps sync-lib");
    for (const l of lib) d.append(el("div", "", l));
    sub.after(d);
  }
  const row = (name, sub, b, cls) => {
    const r = el("div", "row pref wraps " + cls), who = el("div", "who"), val = el("div", "val");
    who.append(el("div", "name", name), el("div", "sub", sub));
    val.append(b);
    r.append(who, val);
    at.append(r);
  };
  if (v.kind === "webdav") sessionTransferRows(at, v, row);
  const peek = el("button", "text", t("Preview…"));
  peek.onclick = async () => {
    peek.classList.add("busy");
    try { askSyncPreview(await api("davsync/preview", {})); } catch (x) { status(t(x.message), "err"); }
    peek.classList.remove("busy");
  };
  row(t("Preview a sync"), t("What syncing now would change, here and on the server. Nothing changes until you sync."), peek, "sync-preview-row");
  if (v.applied?.parts?.length) {
    const undo = el("button", "text", t("Undo…"));
    undo.onclick = () => askUndoSync(v);
    row(t("Undo the last sync"), t("It brought {parts} here {when}. Undo puts back what was here before, on this computer only.", { parts: syncParts(v.applied.parts), when: syncWhen(v.applied.at) }), undo, "sync-undo-row");
  }
};

function sessionTransferRows(at, v, row) {
  if (v.sessionsError) showBatchFailure(at, v.sessionsError);
  const cards = [
    ["Batch session backup and import", "Merge selected conversations, back them up, and import into the agents you choose.", () => window.sessionBatchForm(!!v.followServer)],
    ["Codex session transfer", "Transfer one compressed and encrypted conversation by ID.", () => window.codexSessionForm(!!v.followServer)],
  ];
  for (const [title, subtitle, form] of cards) {
    const button = el("button", "text", t("Open"));
    const panel = el("div");
    button.type = "button";
    button.onclick = () => {
      if (panel.firstChild) panel.replaceChildren();
      else panel.append(form());
      button.textContent = t(panel.firstChild ? "Close" : "Open");
    };
    row(t(title), t(subtitle), button, "sync-session-row");
    at.append(panel);
  }
}

window.codexSessionForm = function (followsServer) {
  const form = el("div", "editor sync-form");
  const field = el("input");
  field.placeholder = t("Session ID");
  field.setAttribute("aria-label", field.placeholder);
  field.autocomplete = "off";
  const download = el("button", "text", t("Download"));
  const upload = el("button", "text", t("Upload…"));
  const message = el("div", "sub wraps");
  message.setAttribute("role", "status");
  const transfer = async (action) => {
    const id = field.value.trim();
    if (!id) { field.focus(); return; }
    upload.disabled = download.disabled = true;
    message.textContent = "";
    message.classList.remove("bad");
    try {
      if (action === "push" && !await confirmAction(t("Upload…"),
        t("The selected conversation may contain private code, prompts and tool output. It will be encrypted before upload."), t("Upload…"))) return;
      const res = await api("davsync/session", { action, id });
      message.textContent = res.changed === 0 && res.skipped > 0 ? t("Remote file matches packed size; skipped sync") : t("Done");
    } catch (err) {
      message.textContent = t(err.message);
      message.classList.add("bad");
    } finally {
      upload.disabled = download.disabled = false;
    }
  };
  upload.onclick = () => transfer("push");
  download.onclick = () => transfer("pull");
  form.append(field, download);
  if (!followsServer) form.append(upload);
  form.append(message);
  return form;
};

window.sessionBatchForm = function (followsServer) {
  const form = el("div", "editor sync-form session-batch-form");
  const sourceList = el("div", "session-batch-sources");
  const search = el("input");
  search.type = "search";
  search.placeholder = t("Filter sessions");
  search.setAttribute("aria-label", search.placeholder);
  const all = batchTick(t("All readable sessions"));
  const agents = batchField(t("Source agent"), "select");
  const everyAgent = el("option", "", t("All"));
  everyAgent.value = "";
  agents.input.append(everyAgent);
  const sources = el("fieldset");
  const filters = el("div", "session-batch-filters", agents.label, search);
  sources.append(el("legend", "", t("Sessions")), all.label, filters, sourceList);

  const back = batchField(t("Existing backup IDs"), "textarea");
  back.input.rows = 2;
  const cwd = batchField(t("Target project folder"), "input");
  const destinations = el("fieldset");
  destinations.append(el("legend", "", t("Import into agents")));
  const backup = el("button", "text", t("Back up"));
  const distribute = el("button", "text primary", t("Back up and import"));
  const syncChanges = el("button", "text", t("Sync local changes"));
  backup.type = distribute.type = syncChanges.type = "button";
  const output = el("div", "session-batch-output sub wraps");
  output.setAttribute("role", "status");
  const actions = el("div", "bar");
  actions.append(backup, distribute);
  if (!followsServer) actions.append(syncChanges);
  form.append(sources, back.label, destinations, cwd.label, actions, output,
    sessionBackupManager(id => {
      back.input.value = [back.input.value.trim(), id].filter(Boolean).join("\n");
      update();
    }));

  let choices = [], targets = [], working = false;
  const ids = () => back.input.value.split(/[\s,]+/).filter(Boolean);
  const chosen = () => choices.filter(c => c.input.checked).map(c => c.ref);
  function update() {
    const ready = ids().length !== 0 || all.input.checked || choices.some(c => c.input.checked);
    backup.disabled = working || !ready;
    distribute.disabled = working || !ready || targets.every(c => !c.input.checked);
    syncChanges.disabled = working || choices.length === 0;
    sourceList.hidden = all.input.checked;
    agents.input.disabled = search.disabled = all.input.checked;
  }
  function filter() {
    const needle = search.value.trim().toLocaleLowerCase();
    for (const c of choices) {
      c.label.hidden = !c.text.includes(needle) || (agents.input.value && c.ref.agent !== agents.input.value);
    }
  }
  form.addEventListener("input", update);
  search.addEventListener("input", filter);
  agents.input.addEventListener("change", filter);
  update();
  api("davsync/session-candidates", {}).then((r) => {
    choices = r.sources.map(({ agent, id, wsl, title, cwd }) => {
      const label = [agent, wsl, title || id, cwd].filter(Boolean).join(" · ");
      const tick = batchTick(label);
      tick.ref = { agent, id, wsl };
      tick.text = (label + " " + id).toLocaleLowerCase();
      return tick;
    });
    sourceList.replaceChildren(...choices.map(c => c.label));
    for (const agent of [...new Set(choices.map(c => c.ref.agent))].sort()) {
      const option = el("option", "", agent);
      option.value = agent;
      agents.input.append(option);
    }
    if (!choices.length) {
      all.input.disabled = true;
      sourceList.append(el("div", "sub", t("No readable sessions found")));
    }
    targets = r.targets.map(agent => {
      const tick = batchTick(agent === "codex" ? "Codex" : "Claude Code");
      tick.agent = agent;
      return tick;
    });
    destinations.append(...targets.map(c => c.label));
    filter();
    update();
  }).catch(err => showBatchFailure(output, err.message));

  async function run(importing) {
    const req = {
      all: all.input.checked,
      sources: all.input.checked ? [] : chosen(),
      backups: ids(),
      targets: importing ? targets.filter(c => c.input.checked).map(c => c.agent) : [],
      cwd: importing ? cwd.input.value.trim() : "",
    };
    working = true;
    update();
    output.replaceChildren();
    output.classList.remove("bad");
    try {
      if ((req.all || req.sources.length) && !await confirmAction(
        t("Batch session backup and import"),
        t("The selected conversations may include private source code, prompts and tool output. Each backup is encrypted."),
        t("Back up"))) return;
      output.textContent = t("Working…");
      const result = await api("davsync/session-batch", req);
      output.replaceChildren();
      showBatchResult(output, result);
    } catch (err) {
      output.replaceChildren();
      showBatchFailure(output, err.message);
    } finally {
      working = false;
      update();
    }
  }
  backup.onclick = () => run(false);
  distribute.onclick = () => run(true);
  syncChanges.onclick = async () => {
    working = true;
    update();
    output.replaceChildren();
    output.classList.remove("bad");
    try {
      if (!await confirmAction(t("All readable sessions"),
        t("The selected conversations may include private source code, prompts and tool output. Each backup is encrypted."),
        t("Sync local changes"))) return;
      output.textContent = t("Working…");
      const result = await api("davsync/session-sync", {});
      output.replaceChildren();
      showBatchResult(output, result);
    } catch (err) {
      output.replaceChildren();
      showBatchFailure(output, err.message);
    } finally {
      working = false;
      update();
    }
  };
  return form;
};

function batchTick(text) {
  const label = el("label", "tick session-batch-choice"), input = el("input");
  input.type = "checkbox";
  label.append(input, el("span", "", text));
  return { label, input };
}

function batchField(text, type) {
  const label = el("label", "session-batch-field"), input = el(type);
  input.autocomplete = "off";
  label.append(el("span", "", text), input);
  return { label, input };
}

function showBatchFailure(out, error) {
  out.append(el("div", "sub bad wraps session-batch-error", t(error)));
}

function showBatchResult(out, r) {
  if (r.backup) {
    const line = el("div", "session-batch-backup");
    const code = el("code", "", r.backup);
    const clipboard = el("button", "text", t("Copy"));
    clipboard.onclick = () => copy(r.backup, clipboard);
    line.append(el("span", "", t("Session backup ID")), code, clipboard);
    out.append(line);
    if (r.remote) {
      if (r.uploaded === 0 && r.skipped > 0) {
        out.append(el("div", "", t("Remote files match packed size · skipped {skipped}", { skipped: r.skipped })));
      } else if (r.uploaded > 0 || r.skipped > 0) {
        out.append(el("div", "", t("WebDAV sync · uploaded {uploaded} · skipped {skipped}", { uploaded: r.uploaded, skipped: r.skipped })));
      } else {
        out.append(el("div", "", t("Backup available on WebDAV")));
      }
    }
  }
  if (r.conversations) out.append(el("div", "", t("{count} conversations · {duplicates} duplicates merged",
    { count: r.conversations, duplicates: r.duplicates })));
  for (const item of r.imports || []) {
    if (item.error) {
      showBatchFailure(out, item.agent + " · " + item.error);
      continue;
    }
    const line = el("div", "session-batch-result");
    const code = el("code", "", item.agent + " · " + item.session);
    const resume = el("button", "text", t("Copy resume command"));
    resume.onclick = () => copy(item.command, resume);
    line.append(code);
    if (item.existing) line.append(el("span", "", t("Reused existing session")));
    line.append(resume);
    out.append(line);
  }
  if (r.failure) showBatchFailure(out, r.failure);
}

function sessionBackupManager(use) {
  const box = el("details", "session-batch-manager");
  const list = el("div", "sub wraps");
  const refresh = el("button", "text", t("Refresh"));
  refresh.type = "button";
  box.append(el("summary", "", t("Manage session backups")), refresh, list);

  async function remove(id, scope) {
    const where = scope === "local" ? t("Delete local backup") : t("Delete WebDAV backup");
    const text = scope === "local" ? t("Delete this backup on this computer? Imported Agent sessions will stay.") :
      t("Delete this backup from WebDAV for every device? Local copies and imported Agent sessions will stay.");
    if (!await confirmAction(where, text, where)) return;
    try {
      await api("davsync/session-backup-delete", { id, scope });
      await load();
    } catch (e) {
      showBatchFailure(list, e.message);
    }
  }

  async function load() {
    refresh.disabled = true;
    list.replaceChildren(el("div", "", t("Working…")));
    try {
      const state = await api("davsync/session-backups", {});
      list.replaceChildren();
      if (!state.items.length) list.append(el("div", "", t("No session backups")));
      for (const item of state.items) {
        const row = el("div", "session-batch-backup");
        row.append(el("code", "", item.id));
        if (item.updated) row.append(el("span", "", new Date(item.updated).toLocaleString()));
        const pick = el("button", "text", t("Use backup"));
        pick.type = "button";
        pick.onclick = () => use(item.id);
        row.append(pick);
        if (item.local) {
          const local = el("button", "text", t("Delete local backup"));
          local.type = "button";
          local.onclick = () => remove(item.id, "local");
          row.append(local);
        }
        if (item.webdav && state.canDeleteWebDAV) {
          const webdav = el("button", "text", t("Delete WebDAV backup"));
          webdav.type = "button";
          webdav.onclick = () => remove(item.id, "webdav");
          row.append(webdav);
        }
        list.append(row);
      }
      if (state.webdavError) showBatchFailure(list, state.webdavError);
    } catch (e) {
      list.replaceChildren();
      showBatchFailure(list, e.message);
    } finally {
      refresh.disabled = false;
    }
  }

  refresh.onclick = load;
  box.ontoggle = () => { if (box.open) load(); };
  return box;
}
