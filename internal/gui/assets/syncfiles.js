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
  const [sessL, sess] = tick(t("Sync readable sessions"), !!v.sessions);
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
  what.append(provL, keysL, setL, agentsL, libL, memL, sessL);
  const adv = el("details", "more advanced sync-advanced");
  adv.open = v.profiles === false || !!v.usage || !!v.signIns;
  const advIn = el("div", "stack");
  advIn.append(profL, useL, siL, exL, siSay, el("div", "hint", t("Usage goes as a file a day: times, tokens, models and costs, never what was said. Turn it on on each computer that shares its own.")));
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
    if (!prof.checked || use.checked || si.checked) adv.open = true;
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
    return r;
  };
  if (v.kind === "webdav") sessionSyncRow(at, v, row);
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

function sessionSyncRow(at, v, row) {
  const button = el("button", "text", t("Open"));
  const panel = el("div");
  button.type = "button";
  const heading = row(t("Session sync"), t("Sync local session changes to WebDAV or import conversations into selected agents."), button, "sync-session-row");
  const sync = el("button", "text", t("Sync sessions now"));
  sync.type = "button";
  if (!v.followServer) button.before(sync);
  const tracker = sessionActivityLine(heading.querySelector(".who"), v, sync, id => {
    if (!panel.firstChild) button.click();
    panel.firstChild.useSessionBackup(id);
  });
  button.onclick = () => {
    if (panel.firstChild) {
      panel.replaceChildren();
      tracker.setFormBlocked(false);
    } else panel.append(window.sessionBatchForm(!!v.followServer, tracker, v));
    button.textContent = t(panel.firstChild ? "Close" : "Open");
  };
  at.append(panel);
}

function sessionActivityLine(at, initial, sync, restore) {
  const note = el("div", "sub wraps say");
  note.setAttribute("role", "status");
  const last = el("button", "text");
  const retry = el("button", "text", t("Retry now"));
  const failures = el("details", "session-backup-failures");
  const failedSummary = el("summary");
  const failedList = el("div", "stack");
  failures.append(failedSummary, failedList);
  failures.hidden = true;
  last.hidden = retry.hidden = true;
  const imported = el("div", "sub wraps");
  at.append(note, imported, last, retry, failures);
  let timer, revision = 0, previousFailures = "", active = false;
  let starting = false, formBlocked = false;
  const updateButton = () => { sync.disabled = starting || active || formBlocked; };
  const paint = a => {
    active = ["preparing", "uploading", "waiting", "importing"].includes(a.phase);
    updateButton();
    const bad = a.error || "";
    const remaining = Math.max(0, Math.ceil((Date.parse(a.retryAt) - Date.now()) / 1000));
    let words = "";
    if (a.phase === "preparing") words = t("Preparing session sync…");
    else if (a.phase === "waiting") words = t("Waiting to retry in {n} s", { n: remaining });
    else if (a.phase === "uploading") words = t("Verified {done} of {count} files · {size} of {total}", { done: a.objects, count: a.count, size: fmtBytes(a.bytes), total: fmtBytes(a.total) });
    else if (a.phase === "importing") words = t("Importing remote sessions…");
    else if (bad) words = tNotice(bad);
    else if (a.phase === "pending") words = t("Session backup upload was interrupted. The next sync will resume it.");
    else if (a.phase === "partial") words = t("{n} conversations synced", { n: a.conversations });
    else if (a.phase === "done" && a.skipped && !a.uploaded) words = t("Remote files match packed size · skipped {skipped}", { skipped: a.skipped });
    else if (a.phase === "done") words = t("{n} conversations synced", { n: a.conversations });
    if (a.phase === "uploading" && a.sending > 0)
      words += " · " + t("Sending {size}", { size: fmtBytes(a.sending) });
    if (note.textContent !== words) note.textContent = words;
    note.hidden = !words;
    note.classList.toggle("bad", !!bad && !active && a.phase !== "pending");
    note.classList.toggle("session-batch-error", !!bad && !active);
    imported.textContent = a.importError ? tNotice(a.importError) : a.importedAt ? t("Last automatic import · {n} sessions · {when}", {n:a.imported, when:new Date(a.importedAt).toLocaleString()}) : "";
    imported.hidden = !imported.textContent;
    imported.classList.toggle("bad", !!a.importError);
    last.hidden = !a.latest;
    if (a.latest) {
      last.textContent = t("Last verified backup · {when}", { when: new Date(a.last).toLocaleString() });
      last.onclick = () => restore(a.latest);
    }
    retry.hidden = initial.followServer || !(a.canRetry || a.phase === "failed" || a.phase === "pending");
    retry.onclick = () => start({ now: true });
    const items = active ? [] : a.failed || [];
    const key = JSON.stringify(items);
    if (key !== previousFailures) {
      previousFailures = key;
      failures.hidden = !items.length;
      failedSummary.textContent = t("{n} sessions to retry", { n: items.length });
      failedList.replaceChildren(...items.map(f => {
        const line = el("div", "sub wraps", (f.title || f.source.id) + " · " + tNotice(f.error));
        const again = el("button", "text", t("Retry"));
        again.type = "button";
        again.onclick = () => start({ source: f.source });
        if (!initial.followServer) line.append(again);
        return line;
      }));
    }
  };
  const schedule = () => {
    clearTimeout(timer);
    if (at.isConnected) timer = setTimeout(poll, active ? 1000 : 15000);
  };
  const poll = async () => {
    if (!at.isConnected) return;
    if (document.hidden || !at.getClientRects().length) {
      schedule();
      return;
    }
    const turn = revision;
    try {
      const a = await api("davsync?view=session-activity");
      if (revision === turn) paint(a);
    } catch (err) {
      if (revision === turn) {
        note.hidden = false;
        note.textContent = tNotice(err.message);
      }
    } finally { schedule(); }
  };
  const start = async body => {
    const turn = ++revision;
    try {
      const a = await api("davsync/session-sync-start", body);
      if (turn === revision) paint(a);
    } catch (err) { statusErr(err); }
    finally { schedule(); }
  };
  paint({ phase: "idle", error: initial.sessionsError });
  timer = setTimeout(poll, 0);
  sync.onclick = async () => {
    if (sync.disabled) return;
    starting = true;
    updateButton();
    try {
      if (await confirmAction(t("All readable sessions"),
        t("The selected conversations may include private source code, prompts and tool output. Each backup is encrypted."),
        t("Sync sessions now"))) await start({});
    } finally {
      starting = false;
      updateButton();
    }
  };
  return { setFormBlocked(blocked) { formBlocked = blocked; updateButton(); } };
}

// A redraw can reopen the form before its last write comes back. Keep the
// latest draft available and order the writes so they cannot arrive backward.
let sessionPrefsDraft, sessionPrefsSaved, sessionPrefsSaving = false;
async function rememberSessionPreferences(prefs) {
  sessionPrefsDraft = prefs;
  if (sessionPrefsSaving) return;
  sessionPrefsSaving = true;
  try {
    while (sessionPrefsDraft !== sessionPrefsSaved) {
      const writing = sessionPrefsDraft;
      try {
        await api("davsync/session-preferences", writing);
        sessionPrefsSaved = writing;
      } catch (e) {
        statusErr(e);
        if (writing === sessionPrefsDraft) return;
      }
    }
  } finally {
    sessionPrefsSaving = false;
  }
}

window.sessionBatchForm = function (followsServer, tracker, sync) {
  const draftAtOpen = sessionPrefsDraft;
  const form = el("div", "editor sync-form session-batch-form");
  const destinations = el("div", "pair wrap session-batch-targets");
  destinations.setAttribute("role", "group");
  destinations.setAttribute("aria-label", t("Import into agents"));
  const distribute = el("button", "text primary", t("Merge and apply"));
  distribute.type = "button";
  const output = el("div", "done session-batch-output sub wraps");
  output.setAttribute("role", "status");
  let restore, targets = [], working = false, loaded = false;
  const selection = el("div", "hint", "");
  const clear = el("button", "text", t("Clear"));
  clear.type = "button";
  const selectedBackup = el("div", "bar", selection, clear);
  const chosenTargets = () => targets.filter(c => c.input.checked).map(c => c.agent);
  const chooseBackup = (id, choice) => {
    restore = { id, entry: choice?.id, title: choice?.title };
    update();
    destinations.querySelector("input")?.focus(NO_SCROLL);
  };
  clear.onclick = () => { restore = undefined; update(); };
  form.useSessionBackup = chooseBackup;
  const autoImport = batchTick(t("Import remote sessions into selected agents on WebDAV sync"));
  const schedule = sync.sessions && !followsServer && sync.auto ? t("Sync local sessions every {n} min", {n: sync.auto}) : t("Automatic sync of local sessions is off");
  const manager = sessionBackupManager(chooseBackup);
  form.append(...field(t("Import into agents"), destinations),
    ...field(t("Automatic sync"), autoImport.label, schedule),
    selectedBackup, el("div", "bar", distribute), output, manager);

  function update() {
    selection.textContent = restore?.title || restore?.id || "";
    selectedBackup.hidden = !restore;
    const hasTargets = chosenTargets().length !== 0;
    autoImport.input.disabled = !loaded || !hasTargets;
    if (loaded && !hasTargets) autoImport.input.checked = false;
    distribute.textContent = t(restore || followsServer ? "Import" : "Merge and apply");
    distribute.disabled = working || !loaded || !hasTargets;
    tracker.setFormBlocked(working);
  }
  form.addEventListener("input", () => {
    update();
    if (loaded) rememberSessionPreferences({ targets: chosenTargets(), autoImport: autoImport.input.checked });
  });
  update();
  api("davsync/session-candidates", {}).then(r => {
    if (!form.isConnected) return;
    const prefs = (sessionPrefsDraft !== sessionPrefsSaved || draftAtOpen !== sessionPrefsDraft
      ? sessionPrefsDraft : r.preferences) || {};
    autoImport.input.checked = !!prefs.autoImport;
    targets = r.targets.map(agent => {
      const tick = batchTick(agent === "codex" ? "Codex" : "Claude Code");
      tick.agent = agent;
      tick.input.checked = (prefs.targets || []).includes(agent);
      return tick;
    });
    destinations.append(...targets.map(c => c.label));
    loaded = true;
    update();
    if (r.preferencesError) showBatchFailure(output, r.preferencesError);
  }).catch(err => showBatchFailure(output, err.message));

  distribute.onclick = async () => {
    const req = {
      current: !restore,
      backups: restore ? [restore.id] : [],
      entry: restore?.entry,
      targets: chosenTargets(),
    };
    working = true;
    update();
    output.replaceChildren();
    output.classList.remove("bad");
    try {
      if (req.current && !followsServer && !await confirmAction(
        t("Session sync"),
        t("All readable conversations will be backed up to WebDAV with encryption."),
        t("Merge and apply"))) return;
      output.textContent = t("Working…");
      const result = await api("davsync/session-batch", req);
      output.replaceChildren();
      showBatchResult(output, result, req);
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
  const [label, input] = tick(text, false);
  label.classList.add("session-batch-choice");
  return { label, input };
}

function showBatchFailure(out, error) {
  out.append(el("div", "sub bad wraps session-batch-error", t(error)));
}

function showBatchResult(out, r, request) {
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
  for (const f of r.failed || []) {
    const line = el("div", "sub wraps", (f.title || f.source.id) + " · " + tNotice(f.error));
    const retry = el("button", "text", t("Retry"));
    retry.type = "button";
    retry.onclick = async () => {
      retry.disabled = true;
      const single = { sources: [f.source], targets: request?.targets || [], cwd: request?.cwd || "" };
      try {
        const result = await api("davsync/session-batch", single);
        line.replaceChildren();
        showBatchResult(line, result, single);
      } catch (err) {
        retry.disabled = false;
        statusErr(err);
      }
    };
    line.append(retry);
    out.append(line);
  }
  if (r.failure) showBatchFailure(out, r.failure);
}

function sessionBackupManager(use) {
  const box = el("details", "more session-batch-manager");
  const list = el("div", "sub wraps");
  const refresh = el("button", "text", t("Refresh"));
  refresh.type = "button";
  const body = el("div", "stack");
  body.append(el("div", "bar", refresh), list);
  box.append(el("summary", "", t("Recoverable backups")), body);

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

  const more = el("button", "text", t("Show more"));
  more.type = "button";
  const status = el("div", "sub wraps");
  status.setAttribute("role", "status");
  body.append(more, status);
  let active;
  const observer = new MutationObserver(() => { if (!box.isConnected) stop(); });
  function stop() {
    active?.abort();
    active = null;
    observer.disconnect();
  }
  function queries(request, signal) {
    return api("davsync/session-restore-choices", request, undefined, signal);
  }
  async function load() {
    stop();
    body.querySelector("[data-older-backups]")?.remove();
    if (!box.open || !box.isConnected) return;
    const controller = new AbortController();
    const { signal } = controller;
    active = controller;
    observer.observe(document.body, {childList:true, subtree:true});
    const known = new Map();
    const rows = new Map();
    let includeOlder = false;
    let limit = 10, previewing = 0, pending = 2, canDelete = false, webPending = true;
    const messages = [];
    list.replaceChildren();
    more.hidden = true;
    status.textContent = t("Loading WebDAV backups…");
    refresh.disabled = false;
    const older=el("button","text",t("Older backups"));
    older.type="button"; older.dataset.olderBackups="";
    older.onclick=()=>{includeOlder=!includeOlder;show();};
    function show() {
      const sorted = [...known.values()].sort((a,b) => Number(!!b.current)-Number(!!a.current) || (Date.parse(b.updated)||0)-(Date.parse(a.updated)||0) || a.id.localeCompare(b.id));
      const hasCurrent=sorted.some(x=>x.current);
      const available=hasCurrent && !includeOlder ? sorted.filter(x=>x.current) : sorted;
      const visible=available.slice(0,limit);
      older.hidden=!hasCurrent || available.length===1 && sorted.length===1;
      older.setAttribute("aria-expanded",String(includeOlder));
      const nodes = visible.map(item => {
        let r = rows.get(item.id);
        if (!r) {
          r = backupRow(item);
          rows.set(item.id,r);
        }
        r.update();
        return r.node;
      });
      // Keep an open row's elements and focus when the other index arrives.
      for (let i=0;i<nodes.length;i++) {
        if (list.children[i] !== nodes[i]) list.insertBefore(nodes[i],list.children[i] || null);
      }
      while (list.children.length > nodes.length) list.lastChild.remove();
      more.hidden = available.length <= limit;
      if (!older.isConnected) status.after(older);
      status.textContent = [...messages, ...(pending ? [t(webPending ? "Loading WebDAV backups…" : "Loading…")] : !sorted.length && !messages.length ? [t("No session backups")] : [])].join(" · ");
      for (const item of visible) {
        if (previewing >= 2) break;
        const row = rows.get(item.id);
        if (row.started) continue;
        row.started = true;
        if (item.current) continue;
        previewing++;
        row.preview().finally(() => {
          previewing--;
          if (!signal.aborted) show();
        });
      }
    }
    function backupRow(item) {
      const node = el("div","stack");
      const row = el("div","session-batch-backup");
      const title = el("span","");
      const date = el("span","sub");
      const inspect = el("button","text",t("View backup"));
      const actions = el("div","stack");
      const local = el("button","text",t("Delete local backup"));
      const remote = el("button","text",t("Delete WebDAV backup"));
      for (const button of [inspect, local, remote]) button.type="button";
      local.onclick=()=>remove(item.id,"local");
      remote.onclick=()=>remove(item.id,"webdav");
      row.append(title,date,inspect,local,remote);
      node.append(row,actions);
      let detail, summary, busy=false;
      const label = () => {
        const data = detail || summary;
        const names = (data?.entries || []).map(e=>e.title).filter(Boolean).slice(0,2);
        title.textContent = item.current ? t("Current session backup") : [data?.device, ...names].filter(Boolean).join(" · ") || item.id.slice(0,12);
        title.title = item.id;
      };
      inspect.onclick = async () => {
        if (busy) return;
        if (detail?.ready) {actions.hidden = !actions.hidden;return;}
        busy=true;
        inspect.disabled=true;
        actions.hidden=false;
        actions.textContent=t("Checking…");
        try {
          const request = source => queries({backup:{...item,local:source==="local",webdav:source==="webdav"},verify:true},signal);
          const remoteFirst=item.webdav && (item.current || !item.local);
          detail=await request(remoteFirst ? "webdav" : "local");
          if (!detail.ready && item.local && item.webdav) detail=await request(remoteFirst ? "local" : "webdav");
          if (signal.aborted) return;
          label();
          actions.replaceChildren();
          if (!detail.ready) {showBatchFailure(actions,detail.problem || t("Incomplete backup"));return;}
          const pick=el("button","text",t("Use backup"));
          pick.type="button";
          pick.onclick=()=>use(item.id);
          actions.append(pick);
          if (detail.entries?.length) actions.append(sessionConversationChoices(detail.entries, choice=>use(item.id,choice)));
        } catch(e) {if (!signal.aborted) {actions.replaceChildren();showBatchFailure(actions,e.message);}}
        finally {busy=false;inspect.disabled=false;}
      };
      return {node, started:false, update() {
        date.textContent=item.updated ? new Date(item.updated).toLocaleString() : "";
        local.hidden=!item.local;
        remote.hidden=!item.webdav || !canDelete;
        label();
      }, async preview() {
        // A preview reads only the authenticated manifest. Prefer disk when
        // available; remote object existence is checked on explicit inspection.
        try {
          const data=await queries({backup:{...item,webdav:!item.local && item.webdav},verify:false},signal);
          if (!signal.aborted){summary=data;label();}
        } catch { /* The row stays usable; inspection shows the current error. */ }
      }};
    }
    more.onclick=()=>{limit+=10;show();};
    await Promise.all(["local","webdav"].map(async scope=>{
      try {
        const index=await queries({scope},signal);
        if (signal.aborted) return;
        if (scope === "webdav") canDelete=index.canDeleteWebDAV;
        for (const next of index.items || []) {
          const item=known.get(next.id) || {id:next.id};
          item.current ||= next.current;
          item.local ||= next.local;
          item.webdav ||= next.webdav;
          if (next.layouts) item.layouts=next.layouts;
          if (next.updated && (!item.updated || Date.parse(next.updated)>Date.parse(item.updated))) item.updated=next.updated;
          known.set(next.id,item);
        }
        if (index.webdavError) messages.push(tNotice(index.webdavError));
      } catch(e) {if (!signal.aborted) messages.push(e.message);}
      finally {pending--;if (scope==="webdav") webPending=false;if (!signal.aborted) show();}
    }));
  }
  refresh.onclick = load;
  box.ontoggle = () => box.open ? load() : stop();
  return box;
}

// The transcript list can contain thousands of conversations. Only create
// controls after it is opened, and render search results a page at a time.
function sessionConversationChoices(entries, use) {
  const choices=el("details","more");
  choices.append(el("summary","",t("Choose a conversation")));
  const stack=el("div","stack");
  const search=input("",t("Filter sessions"));
  search.setAttribute("aria-label",t("Filter sessions"));
  const more=el("button","text",t("Show more"));
  more.type="button";
  let limit=50;
  const draw=()=>{
    const query=search.value.trim().toLocaleLowerCase();
    const matches=entries.filter(e=>(e.title || e.id).toLocaleLowerCase().includes(query));
    stack.replaceChildren();
    for (const choice of matches.slice(0,limit)) {
      const button=el("button","text",choice.title || choice.id);
      button.type="button";button.onclick=()=>use(choice);
      stack.append(button);
    }
    more.hidden=matches.length<=limit;
  };
  if (entries.length>1) choices.append(search);
  choices.append(stack,more);
  choices.ontoggle=()=>{if(choices.open) draw();};
  search.oninput=()=>{limit=50;draw();};
  more.onclick=()=>{limit+=50;draw();};
  return choices;
}
