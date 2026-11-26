// An agent's Long conversations, one row in its sheet (molianyin4227 on
// Discord: models differ, so one size for every agent fits none; the owner,
// 11-12: two rows for one thing, every agent's above this agent's, read as
// the same setting twice). It shows what this agent does, its own value
// else every agent's, and a change is its own (settings.AgentCompacts);
// every agent's row stays in Settings for when no agent holds it. app.js
// imports this when an agent's sheet holds Long conversations, so it weighs
// nothing on the core.
const compactFill = function (list, a) {
  const kind = kindOf(a);
  const row = el("div", "row pref agent-compact");
  row.dataset.agent = kind;
  const who = el("div", "who"), box = el("div");
  // Grok Build compacts at a percent of the window of its own, and with no
  // size typed for it or for every agent, that is where it compacts
  // (settings.ForAgent: @taktoshilite on X, a 1M model compacted at 27%)
  const sub = kind === "grok"
    ? t("Grok Build compacts at 85% of the window, or at the size picked; a smaller size is faster and cheaper. magpie's models only: a subscription's own models keep their own.")
    : t("{agent} compacts at 272K or the size picked; the full window is slower and costlier. magpie's models only: a subscription's own models keep their own.", { agent: a.name });
  who.append(el("div", "name", t("Long conversations")), el("div", "sub", sub));
  row.append(who, box);
  // what Codex's own config holds back, said under the row (renderAgentPrefs)
  const held = el("div", "compact-helds");
  let drawn;
  const draw = () => {
    if (kind === "codex") held.replaceChildren(...list.querySelectorAll("#fullContextSegs .compact-held"));
    // the opened row is built before the settings are read; drawn again then,
    // and only when its values changed, so the page under it holds still
    const s = prefs || {}, own = (s.agentCompacts || {})[kind];
    const key = JSON.stringify([own, s.compactAt, s.fullContext]);
    if (key === drawn) return;
    drawn = key;
    const full = own ? !!own.full : !!s.fullContext || (kind === "grok" && !s.compactAt);
    const at = own ? own.at || 0 : s.compactAt || 0;
    const set = (body) => writingPrefs(api("settings/agent-compact", { agent: kind, own: true, ...body }))
      .then((ns) => { prefs = ns; renderSettings(); if (row.isConnected) draw(); }).catch(settingsErr);
    const size = contextsText({ "*": at || 272000 });
    const sg = segs([["at", t("Compact at {n}", { n: size.toUpperCase() })], ["full", t("Full window")]], full ? "full" : "at",
      (v) => set({ full: v === "full", at }));
    const num = input(at ? size : "", "272k");
    num.className = "words compact-num";
    num.hidden = full;
    num.title = t("Compact at");
    num.setAttribute("aria-label", t("Compact at"));
    num.onchange = () => {
      const r = parseContexts(num.value);
      if (r.error || Object.keys(r.map).some((k) => k !== "*")) return status(t("Compact at: {v} is not a number of tokens like 272k", { v: num.value.trim() }), "err");
      set({ full: false, at: r.map["*"] || 0 });
    };
    const line = el("div", "compact-at");
    line.append(sg, num);
    box.replaceChildren(line, held);
  };
  draw();
  list.querySelector(".agent-compact")?.remove();
  list.querySelectorAll(".claude-cache, .claude-attribution").forEach((r) => r.remove());
  // every agent's row gives its place to this one (app.css .own-compact)
  list.classList.add("own-compact");
  const above = list.querySelector("#fullContextRow");
  above ? above.after(row) : list.append(row);
  if (kind === "claude") {
    const options = claudeOptions(list, a);
    row.after(...claudeCacheRows(options), claudeAttributionRow(options));
  }
  // every agent's values, and Codex's hints, change when that row is drawn
  const seen = new MutationObserver(() => row.isConnected ? draw() : seen.disconnect());
  // Details can build this list before attaching it to the page.
  const segsBox = list.querySelector("#fullContextSegs");
  if (segsBox) seen.observe(segsBox, { childList: true });
};

function claudeOptions(list, a) {
  const remote = a.id.startsWith("claude@wsl:");
  const draws = [];
  let current = null;
  const refresh = (next) => {
    current = next;
    draws.forEach((draw) => draw());
  };
  const report = (err) => { if (list.isConnected) settingsErr(err); };
  if (remote) api("settings/claude-options/" + encodeURIComponent(a.id)).then(refresh, report);
  return {
    get: () => remote ? current : prefs || {},
    watch: (row, draw) => {
      if (remote) draws.push(() => { if (row.isConnected) draw(); });
      else {
        const seen = new MutationObserver(() => row.isConnected ? draw() : seen.disconnect());
        const box = list.querySelector("#fullContextSegs");
        if (box) seen.observe(box, { childList: true });
      }
    },
    set: (path, body, draw) => {
      if (remote) api(path, { agent: a.id, ...body }).then(refresh, report);
      else writingPrefs(api(path, body)).then((ns) => {
        prefs = ns;
        renderSettings();
        draw();
      }).catch(settingsErr);
    },
  };
}

// Claude Code's prompt cache, beside its compaction (Linka on Discord: the
// Usage card offers no way to change one that is already the cheapest):
// the main conversation's and the subagents', each 5 min, 1 hour or
// Claude Code's own default, which takes the key out of its settings.json
// (agent.ClaudeCacheSet). One a variable sets shows it, unchangeable here.
function claudeCacheRows(options) {
  const KNOBS = [["cache_ttl", "Prompt cache · main conversation"], ["subagent_cache_ttl", "Prompt cache · subagents"]];
  const OPTS = [["", "Claude Code's default"], ["5m", "5 min"], ["1h", "1 hour"]];
  return KNOBS.map(([knob, label]) => {
    const row = el("div", "row pref claude-cache");
    row.dataset.knob = knob;
    const who = el("div", "who"), val = el("div", "val");
    who.append(el("div", "name", t(label)));
    row.append(who, val);
    let drawn;
    const draw = () => {
      const own = options.get();
      const c = (own?.claudeCache || []).find((x) => x.knob === knob) || {};
      const key = JSON.stringify([!!own, c]);
      if (key === drawn) return;
      drawn = key;
      const cur = c.value || "", known = OPTS.find(([v]) => v === cur);
      const b = el("button", "sess-pick");
      b.type = "button";
      b.setAttribute("aria-label", t(label));
      b.setAttribute("aria-haspopup", "menu");
      b.setAttribute("aria-expanded", "false");
      b.append(el("span", "", own ? (known ? t(known[1]) : cur) : "—"), svg(CHEV, 11, 1.6));
      b.disabled = !own || !!c.locked;
      b.onclick = (e) => {
        e.stopPropagation();
        if (b.classList.contains("open")) return closeProtoMenu();
        openProtoMenu(b, OPTS.map(([v, name]) => ({ v, name, note: "" })), cur, (v) => {
          if (v === cur) return;
          options.set("settings/claude-cache", { knob, value: v }, () => { if (row.isConnected) draw(); });
        }, label, "sess-menu", "right");
      };
      val.replaceChildren(b);
      who.querySelector(".sub")?.remove();
      if (c.locked) who.append(el("div", "sub", t("{name} is set and comes first", { name: c.locked })));
    };
    draw();
    options.watch(row, draw);
    return row;
  });
}

// Claude Code's Co-Authored-By trailer on the commits it makes (9_bishi on
// X): On is Claude Code's own default, Off writes attribution.commit = "" in
// its settings.json (agent.ClaudeAttributionSet). The user's own trailer
// text is shown as it is and not changed here.
function claudeAttributionRow(options) {
  const label = "Commit signature (Co-Authored-By)";
  const row = el("div", "row pref claude-attribution");
  const who = el("div", "who"), val = el("div", "val");
  who.append(el("div", "name", t(label)));
  row.append(who, val);
  let drawn;
  const draw = () => {
    const own = options.get();
    const cur = own ? own.claudeAttribution || "on" : "";
    if (cur === drawn) return;
    drawn = cur;
    const b = el("button", "sess-pick");
    b.type = "button";
    b.setAttribute("aria-label", t(label));
    b.setAttribute("aria-haspopup", "menu");
    b.setAttribute("aria-expanded", "false");
    b.append(el("span", "", cur ? t(cur === "off" ? "Off" : cur === "custom" ? "Your own text" : "On") : "—"), svg(CHEV, 11, 1.6));
    b.disabled = !cur || cur === "custom";
    b.onclick = (e) => {
      e.stopPropagation();
      if (b.classList.contains("open")) return closeProtoMenu();
      openProtoMenu(b, [["on", "On"], ["off", "Off"]].map(([v, name]) => ({ v, name, note: "" })), cur, (v) => {
        if (v === cur) return;
        options.set("settings/claude-attribution", { on: v === "on" }, () => { if (row.isConnected) draw(); });
      }, label, "sess-menu", "right");
    };
    val.replaceChildren(b);
  };
  draw();
  options.watch(row, draw);
  return row;
}

// The user's own settings of the agent's (agent.Field.Own: Claude Code's
// permission mode, EricChenChannel on X), seldom changed, so folded under
// Advanced after the sheet's list; written in the agent's own config, never
// taken back when magpie lets go of the agent.
function ownFieldsFold(list, a) {
  const own = a.own || [];
  if (!own.length) return;
  queueMicrotask(() => {
    if (!list.parentNode) return;
    list.parentNode.querySelector(":scope > .own-fields")?.remove();
    const fold = el("details", "prefs-more own-fields");
    const inner = el("div", "list prefs");
    fold.append(el("summary", "", t("Advanced")), inner);
    for (const f of own) {
      const label = f.label[0].toUpperCase() + f.label.slice(1);
      const row = el("div", "row pref own-field");
      row.dataset.key = f.key;
      const who = el("div", "who"), val = el("div", "val");
      who.append(el("div", "name", t(label)));
      row.append(who, val);
      const draw = (cur) => {
        const opts = [{ v: "", name: "Not set", note: "" }, ...f.options.map((o) => ({ v: o.value, name: o.label || o.value, note: o.note || "" }))];
        const known = opts.find((o) => o.v === cur);
        const b = el("button", "sess-pick");
        b.type = "button";
        b.setAttribute("aria-label", t(label));
        b.setAttribute("aria-haspopup", "menu");
        b.setAttribute("aria-expanded", "false");
        b.append(el("span", "", known ? t(known.name) : cur), svg(CHEV, 11, 1.6));
        b.onclick = (e) => {
          e.stopPropagation();
          if (b.classList.contains("open")) return closeProtoMenu();
          openProtoMenu(b, opts, cur, (v) => {
            if (v === cur) return;
            api("set", { agent: a.id, field: f.key, value: v }).then((ns) => {
              state = ns;
              if (row.isConnected) draw(v);
              status(t("Takes effect when {agent} next starts", { agent: a.name }), "ok");
            }, (err) => status(t(err.message), "err"));
          }, label, "sess-menu", "right");
        };
        val.replaceChildren(b);
      };
      draw(f.value || "");
      inner.append(row);
    }
    list.after(fold);
  });
}

// Its own context windows come in the same sheet, last (agentwindow.js).
function nativeAccountRow(a) {
  const row = el("div", "row pref native-account");
  const who = el("div", "who");
  who.append(el("div", "name", t("Use native account")), el("div", "sub", t("Use the account signed in to Codex. Session backups and data sync remain available.")));
  const [toggle, input] = tick(t("Use native account"), a.nativeAccount);
  input.setAttribute("aria-label", t("Use native account"));
  toggle.lastElementChild.hidden = true;
  input.onchange = async () => {
    const on = input.checked;
    input.disabled = true;
    try {
      state = await api("agents/native-account/" + encodeURIComponent(a.id), { on });
      const updated = state.agents.find(item => item.id === a.id);
      if (updated) Object.assign(a, updated);
      status(t("Takes effect when {agent} next starts", { agent: a.name }), "ok");
      if (view === "agents") renderAgents();
    } catch (err) {
      statusErr(err);
    } finally {
      input.checked = a.nativeAccount;
      input.disabled = false;
    }
  };
  row.append(who, el("div", "val"));
  row.lastElementChild.append(toggle);
  return row;
}

window.agentCompactFill = function (list, a, ids) {
  if (a.nativeAccount != null) list.prepend(nativeAccountRow(a));
  if (ids.includes("fullContextRow")) compactFill(list, a);
  ownFieldsFold(list, a);
  if (a.fields.length) import("./agentwindow.js").then(() => agentWindowFill(list, a), () => {});
};

window.askAgentDisconnect = function (a) {
  const ed = el("div", "editor disconnect-ask");
  const head = el("div", "ehead");
  head.append(icon(a.icon), el("b", "", t("Disconnect {agent} from magpie?", { agent: a.name })));
  ed.append(head);
  ed.append(el("p", "lib-confirm", t("{agent} goes back to how it was before it was connected; these are the places put back.", { agent: a.name })));
  // what changes, line by line, read from a dry run on a copy (preview.go);
  // where none can be had, the old words
  const said = el("p", "lib-confirm", t("magpie takes its endpoint, key, models and effort out of {agent}'s config, restoring what it had. Providers and accounts stay.", { agent: a.name }));
  const body = el("div", "ag-diff-wrap");
  const show = (changes) => {
    if (changes?.length) body.replaceChildren(changesList(changes, 1));
    else body.replaceChildren(said);
  };
  if (previews[a.id]) show(previews[a.id]);
  else body.append(el("div", "ag-diff-loading", t("Reading what changes…")));
  let previewBlocked = !!a.native, revision = "", offline = false;
  loadPreview(a).then((preview) => {
    show(preview.changes);
    revision = preview.revision;
    previewBlocked = !!a.native && !revision;
    go.disabled = previewBlocked;
  }, (err) => { if (!a.native) { show(null); return; } previewBlocked = true; go.disabled = true; body.replaceChildren(el("p", "lib-confirm", t(err.message))); });
  ed.append(body, el("p", "ag-note", t("The Library's MCP servers, skills and hooks stay")));
  if (kindOf(a) === "codex") ed.append(el("p", "ag-note", t("magpie's provider table stays, so sessions opened on magpie's models still open")));
  const bar = el("div", "bar");
  const go = el("button", "text primary danger-fill", t("Disconnect and restore"));
  go.disabled = previewBlocked;
  go.onclick = async (e) => {
    e.stopPropagation();
    if (previewBlocked) return;
    go.disabled = true;
    go.classList.add("busy");
    try {
      state = await api("agents/" + (offline ? "disconnect-offline/" : "disconnect/") + a.id, offline ? { revision } : {});
      closeConfirmAsk();
      delete previews[a.id];
      if (agentExpanded === a.id) agentExpanded = null;
      renderAgents();
      const msg = t(offline ? "Saved settings restored; magpie was removed from {agent}" : "{agent} no longer goes through magpie; its own settings are back", { agent: a.name });
      saidOk(msg);
    } catch (err) {
      go.disabled = false;
      go.classList.remove("busy");
      if (!offline && err.code === "runtime_unavailable" && err.offline === "disconnect" && revision) {
        offline = true;
        head.querySelector("b").textContent = t("Restore saved settings and remove magpie?");
        ed.querySelector(".lib-confirm").textContent = t("Restore saved settings and remove magpie without contacting Aside? Close Aside first if it is running.");
        go.textContent = t("Restore saved settings and remove magpie");
        cancel.focus(NO_SCROLL);
        return;
      }
      status(t(err.message), "err");
    }
  };
  const cancel = el("button", "text", t("Cancel"));
  cancel.onclick = cancelAsk;
  bar.append(reopenNote(a), el("span", "grow"), cancel, go);
  ed.append(bar);
  confirmAsk = ed;
  openModal(ed);
  $("#modal").classList.add("lib");
  cancel.focus(NO_SCROLL);
};
