// Whale Watcher — signal board widget (Scriptable app, iOS)
// ----------------------------------------------------------
// Shows the top-ranked consensus setups + the verified track record.
// Setup:
//   1. Install "Scriptable" from the App Store (free).
//   2. New script -> paste this file -> edit the 3 CONFIG lines below.
//   3. Long-press home screen -> add Scriptable widget (medium) -> choose
//      this script.
//
// TOKEN: create a FINE-GRAINED personal access token at
//   github.com/settings/personal-access-tokens -> "Generate new token"
//   - Repository access: ONLY your whale-watcher repo
//   - Permissions: Contents -> Read-only. Nothing else.
// That scoping matters: the token lives on your phone, so it should be able
// to do exactly one thing (read this one repo) and nothing more.

// ---- CONFIG ----
const GITHUB_USER = "YOUR_GITHUB_USERNAME";
const REPO = "whale-watcher";
const TOKEN = "github_pat_PASTE_YOURS_HERE";
// ----------------

const url = `https://api.github.com/repos/${GITHUB_USER}/${REPO}/contents/signal_board.json`;

async function fetchBoard() {
  const req = new Request(url);
  req.headers = {
    "Authorization": `Bearer ${TOKEN}`,
    "Accept": "application/vnd.github.raw+json",
  };
  return JSON.parse(await req.loadString());
}

function edgeLine(track) {
  if (!track) return "no graded calls yet";
  const lo = track.edge_lo_pts, hi = track.edge_hi_pts;
  return `n=${track.n}  edge ${track.edge_pts >= 0 ? "+" : ""}${track.edge_pts.toFixed(1)} pts (CI ${lo.toFixed(1)}..${hi.toFixed(1)})`;
}

async function build() {
  const w = new ListWidget();
  w.setPadding(12, 14, 12, 14);
  let board;
  try {
    board = await fetchBoard();
  } catch (e) {
    w.addText("Whale Watcher: can't reach repo").font = Font.systemFont(12);
    return w;
  }

  const header = w.addText(board.edge_detected ? "SIGNAL BOARD" : "SIGNAL BOARD — EXPERIMENTAL");
  header.font = Font.boldSystemFont(12);
  header.textColor = board.edge_detected ? Color.green() : Color.orange();

  const tr = w.addText(edgeLine(board.track_record));
  tr.font = Font.systemFont(9);
  tr.textColor = Color.gray();
  w.addSpacer(6);

  const rows = (board.board || []).slice(0, 4);
  if (rows.length === 0) {
    w.addText("No qualifying setups right now.").font = Font.systemFont(11);
  }
  for (const r of rows) {
    const line = w.addText(
      `${r.score.toFixed(0)}  ${(r.market || "").slice(0, 30)} → ${r.outcome} @ ${(r.price ?? 0).toFixed(2)}`
    );
    line.font = Font.systemFont(11);
    line.lineLimit = 1;
  }

  w.addSpacer(6);
  const foot = w.addText(board.edge_detected
    ? "edge verified by forward test"
    : "unvalidated — what the signal sees, not what to bet");
  foot.font = Font.italicSystemFont(8);
  foot.textColor = Color.gray();
  return w;
}

const widget = await build();
if (config.runsInWidget) {
  Script.setWidget(widget);
} else {
  widget.presentMedium();
}
Script.complete();
