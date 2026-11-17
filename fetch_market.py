"""
Korea Market Radar - 데이터 수집 스크립트
경로: 레포 루트 /fetch_market.py

방어 원칙
1) 기존 data.json을 먼저 읽어 두고, 항목별 수집 실패 시 이전 값을 유지(stale 표시)
2) 핵심 지표(KOSPI) 수집 실패 시 파일을 아예 덮어쓰지 않음
3) 최신 거래일이 기존 data.json과 같으면(휴장일·주말·중복 실행) 덮어쓰지 않음
   - 수동 강제 갱신: 환경변수 FORCE_UPDATE=1
4) 임시 파일에 쓴 뒤 원자적 교체(os.replace) → 쓰기 도중 오류로 깨진 JSON 방지
"""
import os
import sys
import json
import time
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests

try:
    import FinanceDataReader as fdr
except Exception:  # 라이브러리 설치 실패 대비
    fdr = None

try:
    from pykrx import stock
except Exception:
    stock = None

KST = ZoneInfo("Asia/Seoul")
DATA_PATH = "data.json"
FORCE_UPDATE = os.environ.get("FORCE_UPDATE", "").strip() in ("1", "true", "True", "yes")

INDEX_SYMBOLS = {
    "kospi": "KS11",
    "kosdaq": "KQ11",
    "usdkrw": "USD/KRW",
    "sox": "^SOX",
}
# ^SOX 수집이 실패할 때 순서대로 시도할 대체 심볼
INDEX_FALLBACK_SYMBOLS = {
    "sox": ["SOX", "SOXX"],
}

ETF_LIST = [
    {"ticker": "069500", "name": "KODEX 200", "sector": "시장대표", "group": "core"},
    {"ticker": "396500", "name": "TIGER 반도체TOP10", "sector": "반도체·AI", "group": "growth"},
    {"ticker": "466920", "name": "SOL 조선TOP3플러스", "sector": "조선·방산", "group": "rotation"},
    {"ticker": "466940", "name": "TIGER 은행고배당플러스TOP10", "sector": "금융·밸류업", "group": "rotation"},
    {"ticker": "305720", "name": "KODEX 2차전지산업", "sector": "2차전지", "group": "growth"},
    {"ticker": "244580", "name": "KODEX 바이오", "sector": "바이오", "group": "rotation"},
    {"ticker": "459580", "name": "KODEX CD금리액티브(합성)", "sector": "파킹·현금성", "group": "cash"},
]

GEMINI_MODELS = ["gemini-2.5-flash", "gemini-flash-latest", "gemini-2.5-flash-lite"]

EMPTY_FLOW = {"daily_foreign": 0, "daily_inst": 0, "weekly_foreign": 0, "weekly_inst": 0}


def log(msg: str):
    print(f"[fetch_market] {msg}", flush=True)


def now_kst() -> datetime:
    return datetime.now(KST)


# ---------------------------------------------------------------------------
# 기존 데이터 로드 / 저장
# ---------------------------------------------------------------------------
def load_previous() -> dict:
    if not os.path.exists(DATA_PATH):
        return {}
    try:
        with open(DATA_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        log(f"기존 data.json 읽기 실패(무시): {e}")
        return {}


def atomic_write_json(path: str, payload: dict):
    directory = os.path.dirname(os.path.abspath(path))
    fd, tmp_path = tempfile.mkstemp(prefix=".data_", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        # 다시 읽어 JSON 유효성 확인 후 교체
        with open(tmp_path, "r", encoding="utf-8") as f:
            json.load(f)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# ---------------------------------------------------------------------------
# 가격 데이터
# ---------------------------------------------------------------------------
def safe_read(symbol: str, start_date: str, retries: int = 2):
    if fdr is None:
        return None
    for attempt in range(1, retries + 1):
        try:
            df = fdr.DataReader(symbol, start_date)
            if df is not None and not df.empty and "Close" in df.columns:
                df = df.dropna(subset=["Close"])
                df = df[df["Close"] > 0]
                if len(df) >= 2:
                    return df
            log(f"{symbol}: 빈 데이터 (시도 {attempt}/{retries})")
        except Exception as e:
            log(f"{symbol}: 수집 오류 (시도 {attempt}/{retries}) - {e}")
        time.sleep(1.5 * attempt)
    return None


def get_price_metrics(df, cutoff: str = None):
    """cutoff(YYYY-MM-DD)가 주어지면 그 날짜까지만 사용 → 장중 실행 시 KRX 종목 기준일을 KOSPI와 맞춤."""
    if df is not None and cutoff:
        try:
            df = df[df.index <= pd.Timestamp(cutoff)]
        except Exception:
            pass
    if df is None or len(df) < 2:
        return None
    try:
        curr = float(df["Close"].iloc[-1])
        prev_1d = float(df["Close"].iloc[-2])
        prev_5d = float(df["Close"].iloc[-6]) if len(df) >= 6 else float(df["Close"].iloc[0])
        if prev_1d == 0 or prev_5d == 0:
            return None
        last_idx = df.index[-1]
        date_str = last_idx.strftime("%Y-%m-%d") if hasattr(last_idx, "strftime") else str(last_idx)[:10]
        return {
            "price": round(curr, 2),
            "daily_pct": round((curr - prev_1d) / prev_1d * 100, 2),
            "weekly_pct": round((curr - prev_5d) / prev_5d * 100, 2),
            "date": date_str,
            "stale": False,
        }
    except Exception as e:
        log(f"지표 계산 오류: {e}")
        return None


def with_fallback(new_value, prev_value, default):
    """새 값이 없으면 이전 값을 stale=True로 유지, 이전 값도 없으면 default."""
    if new_value is not None:
        return new_value
    if isinstance(prev_value, dict) and prev_value:
        kept = dict(prev_value)
        kept["stale"] = True
        return kept
    d = dict(default)
    d["stale"] = True
    return d


KRX_INDEX_KEYS = {"kospi", "kosdaq"}


def fetch_indices(start_date: str, prev: dict) -> dict:
    prev_indices = prev.get("indices", {}) or {}
    default = {"price": 0, "daily_pct": 0.0, "weekly_pct": 0.0, "date": None}
    out = {}
    cutoff = None
    for key, symbol in INDEX_SYMBOLS.items():
        metrics = get_price_metrics(safe_read(symbol, start_date), cutoff if key in KRX_INDEX_KEYS else None)
        if key == "kospi" and metrics is not None:
            cutoff = metrics["date"]
        if metrics is None:
            for alt in INDEX_FALLBACK_SYMBOLS.get(key, []):
                metrics = get_price_metrics(safe_read(alt, start_date, retries=1))
                if metrics is not None:
                    metrics["symbol_used"] = alt
                    break
        out[key] = with_fallback(metrics, prev_indices.get(key), default)
    return out


def fetch_sectors(start_date: str, prev: dict, cutoff: str = None) -> list:
    prev_map = {s.get("ticker"): s for s in (prev.get("sectors") or []) if isinstance(s, dict)}
    default = {"price": 0, "daily_pct": 0.0, "weekly_pct": 0.0, "date": None}
    sectors = []
    for item in ETF_LIST:
        metrics = get_price_metrics(safe_read(item["ticker"], start_date), cutoff)
        prev_item = prev_map.get(item["ticker"])
        prev_metrics = None
        if prev_item:
            prev_metrics = {k: prev_item.get(k) for k in ("price", "daily_pct", "weekly_pct", "date")}
        merged = with_fallback(metrics, prev_metrics, default)
        sectors.append({**item, **merged})
    return sectors


# ---------------------------------------------------------------------------
# 투자자별 수급 (pykrx)
# ---------------------------------------------------------------------------
def get_investor_flow(market: str = "KOSPI", cutoff: str = None):
    if stock is None:
        return None
    end_dt = now_kst().strftime("%Y%m%d")
    start_dt = (now_kst() - timedelta(days=14)).strftime("%Y%m%d")
    for attempt in range(1, 3):
        try:
            df = stock.get_market_trading_value_by_date(start_dt, end_dt, market)
            if df is None or df.empty:
                log(f"{market} 수급: 빈 데이터 (시도 {attempt}/2)")
                time.sleep(2)
                continue
            if cutoff:
                df = df[df.index <= pd.Timestamp(cutoff)]  # 장중 실행 시 당일 잠정치 제외
                if df.empty:
                    return None
            if "외국인합계" not in df.columns or "기관합계" not in df.columns:
                log(f"{market} 수급: 예상 컬럼 없음 {list(df.columns)}")
                return None
            df_5d = df.tail(5)
            last_idx = df.index[-1]
            return {
                "daily_foreign": round(float(df["외국인합계"].iloc[-1]) / 1e8, 0),
                "daily_inst": round(float(df["기관합계"].iloc[-1]) / 1e8, 0),
                "weekly_foreign": round(float(df_5d["외국인합계"].sum()) / 1e8, 0),
                "weekly_inst": round(float(df_5d["기관합계"].sum()) / 1e8, 0),
                "date": last_idx.strftime("%Y-%m-%d") if hasattr(last_idx, "strftime") else str(last_idx)[:10],
                "stale": False,
            }
        except Exception as e:
            log(f"{market} 수급 오류 (시도 {attempt}/2): {e}")
            time.sleep(2)
    return None


NAVER_SOSOK = {"KOSPI": "01", "KOSDAQ": "02"}


def get_investor_flow_naver(market: str = "KOSPI", cutoff: str = None):
    """네이버 금융 '투자자별 매매동향(일별)' 백업 수집. 단위: 억 원."""
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return None
    import re
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"}
    bizdate = now_kst().strftime("%Y%m%d")
    rows = []
    for page in (1, 2):
        try:
            res = requests.get(
                "https://finance.naver.com/sise/investorDealTrendDay.naver",
                params={"bizdate": bizdate, "sosok": NAVER_SOSOK.get(market, "01"), "page": page},
                headers=headers, timeout=15,
            )
            res.raise_for_status()
            html = res.content.decode("euc-kr", errors="replace")
            soup = BeautifulSoup(html, "html.parser")
            for tr in soup.select("table tr"):
                tds = [td.get_text(strip=True) for td in tr.find_all("td")]
                if len(tds) < 4 or not re.match(r"^\d{2}\.\d{2}\.\d{2}$", tds[0]):
                    continue
                def n(x):
                    x = x.replace(",", "").replace("+", "")
                    return float(x) if re.match(r"^-?\d+(\.\d+)?$", x) else None
                date = "20" + tds[0].replace(".", "-")
                personal, foreign, inst = n(tds[1]), n(tds[2]), n(tds[3])
                if foreign is None or inst is None:
                    continue
                rows.append({"date": date, "foreign": foreign, "inst": inst})
        except Exception as e:
            log(f"{market} 네이버 수급 오류(page {page}): {type(e).__name__}: {e}")
            break
    if not rows:
        return None
    # 날짜 중복 제거 + 최신순 정렬 + 기준 거래일 이후 데이터(장중) 제외
    uniq = {r["date"]: r for r in rows}
    ordered = sorted(uniq.values(), key=lambda r: r["date"], reverse=True)
    if cutoff:
        ordered = [r for r in ordered if r["date"] <= cutoff]
    if not ordered:
        return None
    last5 = ordered[:5]
    return {
        "daily_foreign": round(ordered[0]["foreign"], 0),
        "daily_inst": round(ordered[0]["inst"], 0),
        "weekly_foreign": round(sum(r["foreign"] for r in last5), 0),
        "weekly_inst": round(sum(r["inst"] for r in last5), 0),
        "date": ordered[0]["date"],
        "source": "naver",
        "stale": False,
    }


def fetch_flow(prev: dict, cutoff: str = None) -> dict:
    prev_flow = prev.get("flow", {}) or {}
    out = {}
    for key, market in (("kospi", "KOSPI"), ("kosdaq", "KOSDAQ")):
        flow = get_investor_flow(market, cutoff)
        if flow is None:
            log(f"{market} 수급: pykrx 실패 → 네이버 금융 백업 시도")
            flow = get_investor_flow_naver(market, cutoff)
        out[key] = with_fallback(flow, prev_flow.get(key), EMPTY_FLOW)
    return out


# ---------------------------------------------------------------------------
# AI 브리핑 (Gemini) - 실패 시 규칙 기반 문장으로 대체
# ---------------------------------------------------------------------------
def build_fallback_briefing(payload: dict) -> dict:
    kospi = payload["indices"]["kospi"]
    kosdaq = payload["indices"]["kosdaq"]
    fx = payload["indices"]["usdkrw"]
    sox = payload["indices"]["sox"]
    flow = payload["flow"]["kospi"]

    sectors = [s for s in payload.get("sectors", []) if not s.get("stale")]
    best_d = max(sectors, key=lambda s: s["daily_pct"], default=None)
    worst_d = min(sectors, key=lambda s: s["daily_pct"], default=None)
    best_w = max(sectors, key=lambda s: s["weekly_pct"], default=None)

    flow_ok = not flow.get("stale")
    daily = (
        f"코스피는 {kospi['daily_pct']:+.2f}% 변동한 {kospi['price']:,.2f}pt, 코스닥은 {kosdaq['daily_pct']:+.2f}%로 마감했으며 "
        f"원/달러 환율은 {fx['price']:,.1f}원({fx['daily_pct']:+.2f}%)"
        + (f", 코스피 외국인 {flow['daily_foreign']:+,.0f}억 원·기관 {flow['daily_inst']:+,.0f}억 원입니다. " if flow_ok else "입니다. ")
    )
    if best_d and worst_d:
        daily += f"섹터 ETF는 {best_d['sector']}({best_d['daily_pct']:+.2f}%)가 강했고 {worst_d['sector']}({worst_d['daily_pct']:+.2f}%)가 약했습니다."

    risk_off = (kospi["weekly_pct"] < -2 or kospi["daily_pct"] < -2
                or (flow_ok and flow["weekly_foreign"] < 0) or fx["weekly_pct"] > 1)
    weekly = (
        f"최근 5거래일 코스피 {kospi['weekly_pct']:+.2f}%, 필라델피아 반도체 {sox['weekly_pct']:+.2f}%"
        + (f", 외국인 누적 {flow['weekly_foreign']:+,.0f}억 원·기관 {flow['weekly_inst']:+,.0f}억 원입니다. " if flow_ok else "입니다. ")
    )
    if risk_off:
        weekly += "외국인 이탈·환율 부담 신호가 있어 추격 매수보다 순환매·방어 업종과 현금 버퍼 비중을 높여 포트폴리오 베타를 낮출 구간입니다."
    else:
        lead = best_w["sector"] if best_w else "주도 섹터"
        weekly += f"{lead} 중심의 주도력이 유지되고 있어 반도체·AI 코어는 유지하되 순환매 속도를 점검하며 비중을 조절할 구간입니다."
    return {"daily_comment": daily.strip(), "weekly_comment": weekly.strip(), "source": "rule"}


def generate_ai_briefing(payload: dict) -> dict:
    fallback = build_fallback_briefing(payload)
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        log("GEMINI_API_KEY 없음 → 규칙 기반 브리핑 사용")
        return fallback

    compact = {
        "indices": payload["indices"],
        "flow": payload["flow"],
        "sectors": [
            {k: s.get(k) for k in ("name", "sector", "daily_pct", "weekly_pct", "stale")}
            for s in payload["sectors"]
        ],
    }
    prompt = (
        "당신은 한국 증시 시니어 리서치 애널리스트입니다. 아래 JSON 데이터만 근거로 "
        "1) 일간 시황 핵심 요약(2문장), 2) 주간 흐름 및 대응 전략(2문장)을 한국어로 작성하세요. "
        "대응 전략은 'AI·반도체 코어 + 비반도체 순환매/방어 업종(조선·방산, 고배당 금융, 바이오) + 파킹·현금성 자산' "
        "바벨 포트폴리오의 비중 조절 관점으로 쓰고, 데이터에 없는 수치나 뉴스는 지어내지 마세요. "
        "stale=true 항목은 이전 거래일 값이므로 언급하지 마세요. "
        "반드시 JSON 형식 {\"daily_comment\": \"...\", \"weekly_comment\": \"...\"} 으로만 응답하세요.\n"
        f"데이터: {json.dumps(compact, ensure_ascii=False)}"
    )
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"responseMimeType": "application/json", "temperature": 0.4},
    }
    headers = {"x-goog-api-key": api_key, "Content-Type": "application/json"}
    for model in GEMINI_MODELS:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        for attempt in range(1, 3):
            try:
                res = requests.post(url, headers=headers, json=body, timeout=40)
                if res.status_code != 200:
                    try:
                        err = res.json().get("error", {})
                        detail = f"{err.get('status', '')} {err.get('message', '')}"[:300]
                    except Exception:
                        detail = res.text[:200]
                    log(f"Gemini {model} 실패 (시도 {attempt}/2): HTTP {res.status_code} {detail}")
                    if res.status_code in (400, 401, 403, 404):
                        break  # 키/모델 문제 → 재시도 무의미, 다음 모델로
                    time.sleep(3)
                    continue
                text = res.json()["candidates"][0]["content"]["parts"][0]["text"]
                clean = text.replace("```json", "").replace("```", "").strip()
                parsed = json.loads(clean)
                daily = str(parsed.get("daily_comment", "")).strip()
                weekly = str(parsed.get("weekly_comment", "")).strip()
                if daily and weekly:
                    log(f"Gemini 브리핑 생성 완료 ({model})")
                    return {"daily_comment": daily, "weekly_comment": weekly, "source": "gemini", "model": model}
                log(f"Gemini {model} 응답에 필드 누락")
            except Exception as e:
                # 키가 섞이지 않도록 예외 타입만 출력
                log(f"Gemini {model} 오류 (시도 {attempt}/2): {type(e).__name__}")
            time.sleep(2)
    return fallback


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------
def main() -> int:
    prev = load_previous()
    start_date = (now_kst() - timedelta(days=30)).strftime("%Y-%m-%d")

    indices = fetch_indices(start_date, prev)
    kospi = indices["kospi"]

    # (1) 핵심 지표 실패 → 기존 파일 보존
    if kospi.get("stale") or not kospi.get("price"):
        log("KOSPI 수집 실패 → data.json을 덮어쓰지 않고 종료합니다.")
        return 0

    trade_date = kospi.get("date")
    prev_trade_date = prev.get("trade_date")

    # (2) 신규 거래일 없음(휴장일·주말·중복 실행) → 기존 파일 보존
    #     단, 같은 거래일이라도 이전 실행에서 일부 항목(수급 등)이 비어 있었다면 다시 채우기 위해 재수집
    prev_stale = (prev.get("data_quality") or {}).get("stale_items") or []
    if prev_trade_date and trade_date and trade_date < prev_trade_date and not FORCE_UPDATE:
        log(f"최신 거래일({trade_date})이 기존 데이터({prev_trade_date})보다 이전 → 갱신 생략.")
        return 0
    if prev_trade_date and trade_date == prev_trade_date and not prev_stale and not FORCE_UPDATE:
        log(f"최신 거래일({trade_date}) 데이터가 이미 완전함 → 휴장일/중복 실행으로 판단, 갱신 생략.")
        return 0

    sectors = fetch_sectors(start_date, prev, trade_date)
    flow = fetch_flow(prev, trade_date)

    payload = {
        "updated_at": now_kst().strftime("%Y-%m-%d %H:%M KST"),
        "trade_date": trade_date,
        "indices": indices,
        "flow": flow,
        "sectors": sectors,
    }

    stale_items = [k for k, v in indices.items() if v.get("stale")]
    stale_items += [f"flow.{k}" for k, v in flow.items() if v.get("stale")]
    stale_items += [s["name"] for s in sectors if s.get("stale")]
    payload["data_quality"] = {
        "stale_items": stale_items,
        "is_today": trade_date == now_kst().strftime("%Y-%m-%d"),
    }

    payload["ai_briefing"] = generate_ai_briefing(payload)

    atomic_write_json(DATA_PATH, payload)
    log(f"data.json 갱신 완료 (거래일 {trade_date}, 이전값 유지 항목 {len(stale_items)}개)")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        # 예상치 못한 오류에도 기존 data.json은 그대로 두고 워크플로를 실패 처리
        log(f"치명적 오류: {type(e).__name__}: {e}")
        sys.exit(1)
