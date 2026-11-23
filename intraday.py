"""ACE 이브닝콜 - 장중 리포트 수집기 (intraday.json)

장 마감 리포트(data.json · history/)와 완전히 분리된 가벼운 장중 스냅샷.
- 실행: 평일 09:30~15:20 사이에만 (그 외 시간은 아무것도 하지 않음)
- KRX 호출 약 15회: 지수 2 + 시장 수급 2 + 업종(분류·업종지수·투자자별 순매수, 일간만) 10 + ETF 시세 1
- 구글 뉴스 3회, Gemini 1회
- 기록(history/)에는 저장하지 않음. 장중 값은 모두 잠정치.
"""
import json
import os
import re
import sys
import time
from datetime import timedelta

import requests

import fetch_market as fm

OUT_PATH = "intraday.json"
INTRA_NEWS_QUERIES = ["코스피 장중", "코스피 오전 외국인", "코스닥 장중"]
INTRA_SECTIONS = [("market", "지수·시장"), ("flow", "수급"), ("sector", "업종·테마"), ("issue", "이슈")]


def log(msg):
    fm.log(f"[장중] {msg}")


def in_session(now) -> bool:
    return now.weekday() < 5 and (9, 30) <= (now.hour, now.minute) < (15, 20)


def etf_rows(trade: str) -> list:
    names = fm.etf_names()
    snap = fm.etf_snapshot(trade) if names else None
    if not snap:
        return []
    rows = []
    for code, v in snap.items():
        if code not in names or v["d1"] != v["d1"]:
            continue
        rows.append({"code": code, "name": str(names[code]), "d1": fm.r2(v["d1"]), "w1": None, "m1": None,
                     "price": int(v["price"]) if v["price"] == v["price"] else 0,
                     "value": fm.eok(v["value"]) if v["value"] == v["value"] else 0,
                     "aum": fm.eok(v["aum"]) if v["aum"] == v["aum"] else 0})
    return rows


def slim(p: dict) -> dict:
    """Gemini 입력용 요약."""
    def si(m):
        d = ((p.get("sector_indices") or {}).get(m) or {}).get("daily") or []
        return {"top": [{"n": x["name"], "p": x["pct"], "drv": [y["name"] for y in x.get("drivers", [])]} for x in d[:4]],
                "bottom": [{"n": x["name"], "p": x["pct"], "drv": [y["name"] for y in x.get("drivers", [])]} for x in d[-4:]]}
    ts = (p.get("top_stocks") or {}).get("daily") or {}
    th = p.get("themes_list") or []
    return {
        "time": p["as_of_time"],
        "indices": {k: {"price": v.get("price"), "pct": v.get("daily_pct")} for k, v in p["indices"].items() if k in ("kospi", "kosdaq", "usdkrw")},
        "flow": {k: {i: v.get(f"daily_{i}") for i in ("foreign", "inst", "retail")} for k, v in (p.get("flow") or {}).items()},
        "breadth": p.get("breadth"),
        "sector": {"kospi": si("kospi"), "kosdaq": si("kosdaq")},
        "stocks": {inv: {"buy": [x["name"] for x in (ts.get(inv) or {}).get("buy", [])[:4]],
                         "sell": [x["name"] for x in (ts.get(inv) or {}).get("sell", [])[:4]]} for inv in ("foreign", "inst")},
        "themes_top": [{"t": t["theme"], "p": t["bench"]["d1"]} for t in th[:5]],
        "themes_bottom": [{"t": t["theme"], "p": t["bench"]["d1"]} for t in th[-5:]],
    }


def rule_brief(p: dict) -> dict:
    k, q = p["indices"]["kospi"], p["indices"]["kosdaq"]
    f = (p.get("flow") or {}).get("kospi") or {}
    def dirw(v):
        return "상승" if (v or 0) > 0 else "하락" if (v or 0) < 0 else "보합"
    head = f"코스피 {k.get('daily_pct', 0):+.2f}% {dirw(k.get('daily_pct'))}, 코스닥 {q.get('daily_pct', 0):+.2f}%"
    summary = [f"{p['as_of_time']} 기준 코스피 {k.get('price', 0):,.2f}pt ({k.get('daily_pct', 0):+.2f}%)",
               f"코스피 외국인 {fm.fmt_eok(f.get('daily_foreign') or 0)}, 기관 {fm.fmt_eok(f.get('daily_inst') or 0)} (잠정)"]
    th = p.get("themes_list") or []
    if len(th) >= 2:
        summary.append(f"강세 테마 {th[0]['theme']} {th[0]['bench']['d1']:+.2f}%, 약세 테마 {th[-1]['theme']} {th[-1]['bench']['d1']:+.2f}%")
    return {"headline": head, "summary": summary, "sections": [], "checkpoints": [], "source": "rule"}


def gemini_brief(p: dict) -> dict:
    fallback = rule_brief(p)
    key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not key:
        return fallback
    news = [f"[{n['id']}] {n['title']} ({n['source']}, {n['published']})" for n in (p.get("news") or {}).get("items", [])]
    prompt = (
        "당신은 한국 증시 시니어 애널리스트입니다. 아래는 오늘 장중(마감 전) 잠정 데이터입니다. '장중 리포트'를 작성하세요.\n"
        "1) headline: 지금까지 시장의 결론 한 문장(35자 내외).\n"
        "2) summary: 핵심 3개, 각 1문장. ①지수 방향과 원인 ②수급(누가 사고 파나) ③업종·테마 쏠림.\n"
        "3) sections: market/flow/sector/issue 각 {\"gist\":요지 20자, \"items\":[{\"k\":2~8자 항목명,\"v\":수치 포함 35자}]} 2~3개. issue는 기사 [번호] 인용.\n"
        "4) checkpoints: 오후장·마감까지 볼 것 2~3개.\n"
        "- 모든 문장은 명사로 끝맺음(예: '외국인 3,200억 원 순매도', '반도체 약세'). '~함/~됨/~다' 금지.\n"
        "- 장중 잠정치임을 전제로 단정 표현 자제(예: '마감' 대신 '오전장', '현재'). 데이터에 없는 수치 금지, 기사 수치보다 데이터 우선.\n"
        "- 수급 단위 억 원, 1만억 이상은 조 단위. 지수는 5,100.50pt 형식. 투자 권유 표현 금지. 특정 운용사 상품명 언급 금지.\n"
        '반드시 JSON으로만: {"headline":"...","summary":["..."],"sections":{"market":{...},"flow":{...},"sector":{...},"issue":{...}},"checkpoints":["..."]}\n\n'
        f"[데이터]\n{json.dumps(slim(p), ensure_ascii=False)}\n\n[기사]\n" + ("\n".join(news) if news else "(없음)")
    )
    S_, A_ = {"type": "STRING"}, (lambda it: {"type": "ARRAY", "items": it})
    O_ = lambda props, req=None: {"type": "OBJECT", "properties": props, "required": req if req is not None else list(props)}
    sec = O_({"gist": S_, "items": A_(O_({"k": S_, "v": S_}))})
    schema = O_({"headline": S_, "summary": A_(S_), "sections": O_({k: sec for k, _ in INTRA_SECTIONS}, []), "checkpoints": A_(S_)})
    body = {"contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"responseMimeType": "application/json", "responseSchema": schema, "temperature": 0.3, "maxOutputTokens": 8192}}
    headers = {"x-goog-api-key": key, "Content-Type": "application/json"}
    for model in fm.GEMINI_MODELS:
        try:
            res = requests.post(f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
                                headers=headers, json=body, timeout=90)
            if res.status_code != 200:
                log(f"Gemini {model} HTTP {res.status_code} → 다음 모델")
                time.sleep(2)
                continue
            parsed = json.loads(res.json()["candidates"][0]["content"]["parts"][0]["text"])
            sections = []
            for k, title in INTRA_SECTIONS:
                s = fm.normalize_section((parsed.get("sections") or {}).get(k))
                s["items"] = [it for it in s["items"] if not fm.OTHER_BRANDS.search(it["k"] + " " + it["v"])]
                if s["items"]:
                    sections.append({"key": k, "title": title, **s})
            out = fm.polish_brief({"headline": str(parsed.get("headline", "")).strip(), "summary": fm.clean_list(parsed.get("summary"), 4),
                                   "sections": sections, "strategy": fm.clean_list(parsed.get("checkpoints"), 3)})
            if not out["headline"] or len(out["summary"]) < 2:
                raise ValueError("형식 불충분")
            out["checkpoints"] = out.pop("strategy")
            out["source"], out["model"] = "gemini", model
            log(f"Gemini 장중 브리핑 완료 ({model})")
            return out
        except Exception as e:
            log(f"Gemini {model} 오류: {type(e).__name__}: {str(e)[:120]}")
    return fallback


def main() -> int:
    now = fm.now_kst()
    if not in_session(now) and not fm.FORCE_UPDATE:
        log(f"장중 리포트는 평일 09:30~15:20에만 → 생략 ({now:%m-%d %H:%M})")
        return 0
    today = now.strftime("%Y-%m-%d")
    fm.krx_login_check()
    start = (now - timedelta(days=45)).strftime("%Y-%m-%d")
    indices, kospi_df = fm.fetch_indices(start, {}, intraday=True)
    k = indices.get("kospi") or {}
    if k.get("stale") or k.get("date") != today or kospi_df is None:
        log(f"오늘({today}) 장중 지수 없음 (받은 기준일 {k.get('date')}) → 휴장일 또는 KRX 미제공, intraday.json 유지")
        return 0
    dates = [d.strftime("%Y-%m-%d") for d in kospi_df.index]
    hm = now.strftime("%H:%M")

    def guarded(label, fn, default):
        try:
            return fn()
        except Exception as e:
            log(f"{label} 실패(생략): {type(e).__name__}: {e}")
            return default

    flow = guarded("시장 수급", lambda: {key: fm.get_market_flow(mk, today) for key, mk in fm.MARKETS}, {})
    si, sf, ts, br = guarded("업종", lambda: fm.fetch_sector_block(dates, {}, periods=("daily",)), ({}, {}, {}, {}))
    rows = guarded("ETF", lambda: etf_rows(today), [])
    themes = guarded("테마", lambda: fm.build_themes(rows, "d1"), []) if rows else []
    fm.NEWS_QUERIES = INTRA_NEWS_QUERIES
    news = guarded("뉴스", lambda: fm.fetch_news({}, today), {"items": []})

    if not flow or not any(flow.values()):
        log("장중 수급 수집 실패 → 기존 intraday.json 유지")
        return 0
    payload = {
        "kind": "intraday",
        "updated_at": now.strftime("%Y-%m-%d %H:%M KST"),
        "trade_date": today,
        "as_of_time": hm,
        "indices": indices,
        "flow": flow,
        "breadth": br,
        "sector_indices": si,
        "sector_flow": sf,
        "top_stocks": ts,
        "themes": {"daily": [{key: t[key] for key in ("theme", "bench", "bundle", "count", "ace_count", "ace", "others")} for t in themes]},
        "news": news,
        "data_quality": {"final": False, "is_today": True, "intraday": True, "stale_items": []},
        "as_of": {
            "index": f"한국거래소 {today} {hm} 현재 (장중 잠정)",
            "fx": f"원/달러 {hm} 수집 (FinanceDataReader)",
            "flow": f"한국거래소 투자자별 매매 · {hm} 수집 · 장중 잠정치",
            "breadth": f"한국거래소 {hm} 기준 종목 등락",
            "sector": f"한국거래소 업종지수·업종 수급 {hm} 수집 · 장중 잠정",
            "stock": f"한국거래소 투자자별 순매수 · {hm} 수집 · 장중 잠정",
            "etf": f"한국거래소 ETF {hm} 시세 · 장중 잠정",
            "news": f"구글 뉴스 {today} 08:00 ~ {hm}",
        },
    }
    payload["themes_list"] = payload["themes"]["daily"]
    brief = guarded("브리핑", lambda: gemini_brief(payload), rule_brief(payload))
    payload.pop("themes_list")
    payload["as_of"]["briefing"] = f"{brief.get('model') or '규칙 기반'} · {hm} 작성"
    payload["ai_briefing"] = {"daily": {k: brief.get(k) for k in ("headline", "summary", "sections")} | {"strategy": brief.get("checkpoints") or []},
                              "source": brief.get("source"), "model": brief.get("model")}
    fm.atomic_write_json(OUT_PATH, payload)
    log(f"intraday.json 갱신 완료 ({today} {hm})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
