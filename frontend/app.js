const $ = id => document.getElementById(id);
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
let submission, active;
function message(id, text, error = false) {
  $(id).textContent = text;
  $(id).classList.toggle("error", error);
}
async function api(path, method = "GET", data, headers = {}) {
  const response = await fetch(path, {method, headers: {"Content-Type": "application/json", ...headers},
    body: data === undefined ? undefined : JSON.stringify(data)});
  const body = await response.json();
  if (!response.ok) throw new Error(Object.values(body.fields || {}).join(". ") || body.error || "Request failed");
  return body;
}
function fields() {return {organism: Number($("organism").value), motif: $("motif").value};}
function fill(query) {
  $("organism").value = query.organism;
  $("motif").value = query.motif;
}
$("wizard").onsubmit = async event => {
  event.preventDefault();
  const button = event.submitter;
  button.disabled = true;
  try {
    const draft = await api("/api/wizard/draft", "POST", {text: $("prompt").value});
    if (draft.clarification) return message("draft-msg", draft.clarification);
    fill((await api("/api/query/validate", "POST", draft)).query);
    message("draft-msg", "Draft ready. Review the fields below.");
    $("organism").focus();
  } catch (err) {message("draft-msg", err.message, true);}
  finally {button.disabled = false;}
};
async function watch(path) {
  while (active === path) {
    const run = await api(path);
    if (active !== path) return;
    $("status").textContent = {complete:"Complete",needs_resume:"Needs resume",running:"Running"}[run.status];
    $("detail").textContent = "Taxon " + run.query.organism + " · Motif " + run.query.motif;
    for (const [name, src] of Object.entries(run.sources)) {
      $(name).textContent = src.pages_completed + " pages · " + (src.complete ? "Complete" : src.paused ? "Paused" : "In progress");
    }
    $("resume").hidden = !run.can_resume;
    document.querySelectorAll("[data-format]").forEach(btn => btn.disabled = !run.can_download);
    if (run.can_download || run.can_resume) return;
    await wait(450);
  }
}
$("manual").onsubmit = async event => {
  event.preventDefault();
  const button = event.submitter;
  button.disabled = true;
  try {
    const raw = fields();
    const query = (await api("/api/query/validate", "POST", raw)).query;
    if (raw.motif !== query.motif) {
      fill(query);
      return message("run-msg", "Motif normalized. Review it and click Run query to confirm.");
    }
    const signature = JSON.stringify(query);
    if (submission?.signature !== signature) submission = {signature, key: crypto.randomUUID()};
    const result = await api("/api/query", "POST", query, {"Idempotency-Key": submission.key});
    submission = undefined;
    active = result.status_url;
    document.querySelectorAll("[data-format]").forEach(btn => btn.disabled = true);
    message("run-msg", "Query submitted.");
    message("download-msg", "");
    await watch(active);
  } catch (err) {message("run-msg", err.message, true);}
  finally {button.disabled = false;}
};
$("resume").onclick = async () => {
  try {await api(active + "/resume", "POST"); await watch(active);}
  catch (err) {message("run-msg", err.message, true);}
};
for (const btn of document.querySelectorAll("[data-format]")) {
  btn.onclick = async () => {
    const path = active;
    const url = path + "/download?format=" + btn.dataset.format;
    btn.disabled = true;
    try {
      let result = await api(url, "POST");
      while (result.status === "preparing" && active === path) {
        message("download-msg", "Preparing CSV…");
        await wait(400);
        result = await api(url);
      }
      if (active !== path) return;
      if (result.status !== "ready") throw new Error("Conversion stopped. Click Download CSV to retry.");
      const link = document.createElement("a");
      link.href = result.url; link.download = ""; link.click();
      message("download-msg", "Download ready.");
    } catch (err) {message("download-msg", err.message, true);}
    finally {if (active === path) btn.disabled = false;}
  };
}
