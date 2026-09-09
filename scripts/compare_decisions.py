"""Read-only comparison of persisted SIM and live decision evidence."""
import argparse
import json
import sqlite3
from pathlib import Path


def brief(sample):
    return {k: sample.get(k) for k in ("evaluationId", "stream", "trigger", "reason",
        "observedAt", "remainingSeconds", "matchesLatestWs", "details", "settings")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--window")
    parser.add_argument("--variant", default="btc-historical-hybrid")
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()
    live = json.loads((args.root / "polymarket_live_strategy_state.json").read_text(encoding="utf-8"))
    with sqlite3.connect((args.root / "polymarket_sim.sqlite3").resolve().as_uri()+"?mode=ro", uri=True) as db:
        sql = "SELECT diagnostic_json FROM sim_window_diagnostics WHERE variant_id=?"
        params = [args.variant]
        if args.window:
            sql += " AND window_slug=?"
            params.append(args.window)
        sql += " ORDER BY last_seen DESC LIMIT ?"
        params.append(max(1, min(50, args.limit)))
        windows = [json.loads(row[0]) for row in db.execute(sql, params)]
    for sim in windows:
        matching = [x for x in live.get("windowDiagnostics", [])
                    if x.get("windowSlug")==sim["windowSlug"] and x.get("strategyVariant")==args.variant]
        live_samples = [s for x in matching for key in ("decisionSamples", "decisionEvents") for s in x.get(key, [])]
        events = sim.get("decisionEvents", [])
        # Without entries, show the latest sampled evaluation for diagnosis.
        targets = events or sim.get("decisionSamples", [])[-1:]
        comparisons = []
        for s in targets:
            compatible = [x for x in live_samples if x["runId"]==s["runId"]]
            nearest = min(compatible, key=lambda x: abs(x["observedAt"]-s["observedAt"])) if compatible else None
            comparisons.append({"sim": brief(s), "nearestLiveSample": brief(nearest) if nearest else None,
                "timeDifferenceMs": round((nearest["observedAt"]-s["observedAt"])*1000, 3) if nearest else None,
                "sameBooks": all(s[k]["fingerprint"]==nearest[k]["fingerprint"] for k in ("upBook","downBook")) if nearest else None})
        print(json.dumps({"window": sim["windowSlug"], "simStatus": sim.get("status"),
            "liveStatus": [x.get("status") for x in matching],
            "simSupersededSamples": sum(not x.get("matchesLatestWs", True) for x in sim.get("decisionSamples", [])),
            "comparisons": comparisons, "note": "Nearest sampled evaluation is not proof of the same event; inspect fingerprints and time difference."}, ensure_ascii=False))


if __name__ == "__main__":
    main()
