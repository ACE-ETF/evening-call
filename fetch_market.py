"""
Korea Market Radar - 데이터 수집 스크립트
경로: 레포 루트 /fetch_market.py

수집 항목
- 지수: KOSPI, KOSDAQ, USD/KRW, SOX (+20거래일 추이)
- 투자자 수급: 코스피·코스닥 외국인/기관/개인 (일간·5일 누적)
- 업종지수 등락률: 코스피·코스닥 업종지수 (일간·5일)
- 업종별 수급: 외국인/기관/개인 순매수를 업종별로 집계 (일간·5일)
- 순매수 상·하위 종목: 투자자별 (일간·5일)
- 시장 폭: 상승/하락/보합 종목 수
- 섹터 ETF: 대표 ETF 등락률
- 관련 기사: 구글 뉴스 RSS 헤드라인
- 시황 브리핑: Gemini (두괄식·개조식, 기사 번호 인용) / 실패 시 규칙 기반

방어 원칙
1) 기존 data.json을 먼저 읽어 두고, 항목별 수집 실패 시 이전 값을 유지(stale 표시)
2) 핵심 지표(KOSPI) 수집 실패 시 파일을 아예 덮어쓰지 않음
3) 같은 거래일 데이터가 이미 완전하면(휴장일·주말·중복 실행) 덮어쓰지 않음
   - 이전 실행에 빈 항목이 있었다면 같은 거래일이라도 재수집
   - 수동 강제 갱신: 환경변수 FORCE_UPDATE=1
4) 임시 파일에 쓴 뒤 원자적 교체(os.replace) → 쓰기 도중 오류로 깨진 JSON 방지
5) 장중 실행 시 모든 국내 데이터를 KOSPI 기준 거래일(전일 종가)에 맞춤
"""
import os
import re
import sys
import json
import time
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
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
INDEX_FALLBACK_SYMBOLS = {"sox": ["SOX", "SOXX"]}
KRX_INDEX_KEYS = {"kospi", "kosdaq"}

ETF_LIST = [
    {"ticker": "069500", "name": "KODEX 200", "sector": "시장대표"},
    {"ticker": "396500", "name": "TIGER 반도체TOP10", "sector": "반도체·AI"},
    {"ticker": "466920", "name": "SOL 조선TOP3플러스", "sector": "조선·방산"},
    {"ticker": "466940", "name": "TIGER 은행고배당플러스TOP10", "sector": "금융·밸류업"},
    {"ticker": "305720", "name": "KODEX 2차전지산업", "sector": "2차전지"},
    {"ticker": "244580", "name": "KODEX 바이오", "sector": "바이오"},
    {"ticker": "459580", "name": "KODEX CD금리액티브(합성)", "sector": "파킹·현금성"},
]

MARKETS = [("kospi", "KOSPI"), ("kosdaq", "KOSDAQ")]
# pykrx 투자자 구분명 → data.json 키
INVESTORS = [("foreign", "외국인"), ("inst", "기관합계"), ("retail", "개인")]
INVESTOR_LABEL = {"foreign": "외국인", "inst": "기관", "retail": "개인"}

NEWS_QUERIES = ["코스피 마감시황", "코스닥 마감 외국인"]
NEWS_MAX = 8
NEWS_POST_CLOSE_MIN = 4   # 장 마감(15:30) 이후 기사가 이 개수 미만이면 17시대에 기사·브리핑 보강
NEWS_REFRESH_UNTIL = 19   # 이 시각(KST) 이후에는 기사 부족해도 보강 중단
FINAL_HOUR, FINAL_MIN = 18, 5  # 시간외 거래(~18:00) 반영된 수급 확정 시각

GEMINI_MODELS = ["gemini-3.8-flash", "gemini-3.5-flash-lite", "gemini-flash-latest"]

EMPTY_FLOW = {f"{p}_{k}": 0 for p in ("daily", "weekly") for k, _ in INVESTORS}

KRX_PAUSE = 1.0   # KRX 연속 호출 간격(초) - 과도한 호출 방지
NEWS_PAUSE = 4.0  # 뉴스 검색 간격(초) - 구글 429 방지


def log(msg: str):
    print(f"[fetch_market] {msg}", flush=True)


def now_kst() -> datetime:
    return datetime.now(KST)


def ymd(d: str) -> str:
    return d.replace("-", "")


def eok(v) -> float:
    """원 → 억 원"""
    try:
        return int(round(float(v) / 1e8))
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# 기존 데이터 로드 / 저장
# ---------------------------------------------------------------------------
HISTORY_DIR = "history"


def archive(payload: dict):
    """history/YYYY-MM-DD.json 에 그날 최종본 보관 + history/index.json 목록 갱신 (같은 날은 덮어씀)."""
    os.makedirs(HISTORY_DIR, exist_ok=True)
    d = payload["trade_date"]
    atomic_write_json(os.path.join(HISTORY_DIR, f"{d}.json"), payload)
    idx_path = os.path.join(HISTORY_DIR, "index.json")
    try:
        with open(idx_path, encoding="utf-8") as f:
            idx = json.load(f)
    except Exception:
        idx = []
    idx = [x for x in idx if x.get("date") != d]
    k = payload["indices"]["kospi"]
    q = payload["indices"]["kosdaq"]
    fl = payload["flow"]["kospi"]
    idx.append({
        "date": d,
        "kospi": k.get("price"), "kospi_pct": k.get("daily_pct"),
        "kosdaq": q.get("price"), "kosdaq_pct": q.get("daily_pct"),
        "foreign": fl.get("daily_foreign"), "inst": fl.get("daily_inst"), "retail": fl.get("daily_retail"),
        "headline": ((payload.get("ai_briefing") or {}).get("daily") or {}).get("headline", ""),
        "final": payload["data_quality"].get("final"),
        "updated_at": payload["updated_at"],
    })
    idx.sort(key=lambda x: x["date"], reverse=True)
    atomic_write_json(idx_path, idx)
    log(f"날짜별 보관: {HISTORY_DIR}/{d}.json (누적 {len(idx)}일)")


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
        with open(tmp_path, "r", encoding="utf-8") as f:
            json.load(f)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def keep_previous(new_value, prev_value, default):
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


KRX_STATE = {"fails": 0, "down": False}
KRX_MAX_FAILS = 3  # 연속 실패 시 KRX 호출 중단 (로그인 실패 반복으로 계정이 잠기는 것 방지)


def krx_login_check() -> bool:
    """실행 시작 시 KRX 로그인 1회 점검. 실패하면 KRX가 돌려준 오류 코드·메시지를 로그로 남기고
    이번 실행의 KRX 호출을 전부 건너뜀 (실패한 로그인이 반복돼 계정이 잠기는 것 방지)."""
    login_id, login_pw = os.environ.get("KRX_ID", ""), os.environ.get("KRX_PW", "")
    if not (login_id and login_pw):
        log("KRX_ID/KRX_PW 없음 → KRX 호출 건너뜀")
        KRX_STATE["down"] = True
        return False
    try:
        from pykrx.website.comm import auth as krx_auth
        sess = requests.Session()
        krx_auth.warmup_krx_session(sess)
        payload = {"mbrNm": "", "telNo": "", "di": "", "certType": "", "mbrId": login_id, "pw": login_pw}
        headers = {"User-Agent": krx_auth.USER_AGENT, "Referer": krx_auth.LOGIN_PAGE}
        resp = sess.post(krx_auth.LOGIN_URL, data=payload, headers=headers, timeout=15)
        try:
            data = resp.json()
        except Exception:
            log(f"KRX 로그인 점검: JSON 아님 (HTTP {resp.status_code}) {resp.text[:120]!r}")
            KRX_STATE["down"] = True
            return False
        code, msg = data.get("_error_code", ""), data.get("_error_message", "")
        if code == "CD011":  # 중복 로그인 → 기존 세션 끊고 재시도
            payload["skipDup"] = "Y"
            data = sess.post(krx_auth.LOGIN_URL, data=payload, headers=headers, timeout=15).json()
            code, msg = data.get("_error_code", ""), data.get("_error_message", "")
        log(f"KRX 로그인 점검: 코드 {code or '-'} / {msg or '-'} (ID 길이 {len(login_id)}, PW 길이 {len(login_pw)})")
        if code == "CD001":
            return True
        KRX_STATE["down"] = True
        return False
    except Exception as e:
        log(f"KRX 로그인 점검 오류: {type(e).__name__}: {e}")
        return True  # 점검 자체 오류면 기존 방식으로 진행


def krx_call(label: str, fn, *args, retries: int = 2):
    """pykrx 호출 공통 래퍼: 빈 결과/오류 시 재시도 후 None. 연속 실패가 쌓이면 이후 호출을 모두 건너뜀."""
    if stock is None or fn is None:
        return None
    if KRX_STATE["down"]:
        return None
    df = _krx_call(label, fn, *args, retries=retries)
    if df is None:
        KRX_STATE["fails"] += 1
        if KRX_STATE["fails"] >= KRX_MAX_FAILS:
            KRX_STATE["down"] = True
            log(f"KRX 연속 {KRX_MAX_FAILS}회 실패 → 이번 실행의 KRX 호출 중단 (로그인 정보 KRX_ID/KRX_PW 확인 필요)")
    else:
        KRX_STATE["fails"] = 0
    return df


def _krx_call(label: str, fn, *args, retries: int = 2):
    for attempt in range(1, retries + 1):
        try:
            df = fn(*args)
            time.sleep(KRX_PAUSE)
            if df is not None and not df.empty:
                return df
            log(f"{label}: 빈 데이터 (시도 {attempt}/{retries})")
        except Exception as e:
            log(f"{label}: 오류 (시도 {attempt}/{retries}) - {type(e).__name__}: {e}")
        time.sleep(1.5 * attempt)
    return None


# ---------------------------------------------------------------------------
# 가격 데이터 (FinanceDataReader)
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


def cut(df, cutoff):
    if df is not None and cutoff:
        try:
            return df[df.index <= pd.Timestamp(cutoff)]
        except Exception:
            return df
    return df


def get_price_metrics(df, cutoff: str = None, series_len: int = 0):
    df = cut(df, cutoff)
    if df is None or len(df) < 2:
        return None
    try:
        curr = float(df["Close"].iloc[-1])
        prev_1d = float(df["Close"].iloc[-2])
        prev_5d = float(df["Close"].iloc[-6]) if len(df) >= 6 else float(df["Close"].iloc[0])
        if prev_1d == 0 or prev_5d == 0:
            return None
        out = {
            "price": round(curr, 2),
            "daily_pct": round((curr - prev_1d) / prev_1d * 100, 2),
            "weekly_pct": round((curr - prev_5d) / prev_5d * 100, 2),
            "date": df.index[-1].strftime("%Y-%m-%d"),
            "stale": False,
        }
        if series_len:
            tail = df["Close"].tail(series_len)
            out["series"] = [{"d": i.strftime("%m-%d"), "c": round(float(v), 2)} for i, v in tail.items()]
        return out
    except Exception as e:
        log(f"지표 계산 오류: {e}")
        return None


def cutoff_for(key: str, trade_date: str):
    """국내 기준 거래일에 맞춘 데이터 기준일.
    - KOSPI/KOSDAQ/환율: 기준 거래일까지
    - SOX(미국장): 기준 거래일 '전날'까지 → 국내 장에 영향을 준 전일 밤 미국장 (모닝콜에서 다룬 값)"""
    if not trade_date:
        return None
    if key == "sox":
        return (pd.Timestamp(trade_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    return trade_date


KRX_INDEX_CODES = {"kospi": "1001", "kosdaq": "2001"}


def read_krx_index(key: str, start_date: str):
    """한국거래소 공식 지수 종가 (pykrx). FinanceDataReader는 장 마감 직후 최종값이 아닐 수 있음."""
    if stock is None or key not in KRX_INDEX_CODES:
        return None
    df = krx_call(f"{key} 지수(KRX)", stock.get_index_ohlcv_by_date, ymd(start_date), now_kst().strftime("%Y%m%d"), KRX_INDEX_CODES[key], "d", False)
    if df is None or "종가" not in df.columns:
        return None
    df = df.rename(columns={"종가": "Close"})
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    df = df.dropna(subset=["Close"])
    df = df[df["Close"] > 0]
    return df if len(df) >= 2 else None


def fetch_indices(start_date: str, prev: dict):
    """지수 수집. KOSPI 원본 DataFrame도 함께 반환(거래일 목록 산출용)."""
    prev_indices = prev.get("indices", {}) or {}
    default = {"price": 0, "daily_pct": 0.0, "weekly_pct": 0.0, "date": None}
    out, cutoff, kospi_df = {}, None, None
    for key, symbol in INDEX_SYMBOLS.items():
        df = read_krx_index(key, start_date) if key in KRX_INDEX_CODES else None
        if df is None:
            df = safe_read(symbol, start_date)
        else:
            log(f"{key}: KRX 공식 종가 사용 ({df.index[-1].strftime('%Y-%m-%d')} {float(df['Close'].iloc[-1]):,.2f})")
        metrics = get_price_metrics(df, cutoff_for(key, cutoff), series_len=20)
        if metrics is None:
            for alt in INDEX_FALLBACK_SYMBOLS.get(key, []):
                metrics = get_price_metrics(safe_read(alt, start_date, retries=1), cutoff_for(key, cutoff), series_len=20)
                if metrics is not None:
                    metrics["symbol_used"] = alt
                    break
        if key == "kospi" and metrics is not None and df is not None:
            # 장중(15:40 이전) 실행이면 오늘 잠정 시세 행은 제외하고 직전 거래일 종가 기준으로
            now = now_kst()
            if (now.hour, now.minute) < (15, 40):
                y = (pd.Timestamp(now.strftime("%Y-%m-%d")) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                metrics = get_price_metrics(df, y, series_len=20)
        if key == "kospi" and metrics is not None:
            cutoff = metrics["date"]
            kospi_df = cut(df, cutoff)
        out[key] = keep_previous(metrics, prev_indices.get(key), default)
    return out, kospi_df


INDEX_NEWS_KW = {"kospi": "코스피", "kosdaq": "코스닥"}
CLOSE_WORDS = re.compile(r"마감|종가|마쳤|마쳐|마치|끝냈|끝내|끝마|거래를 마")
NUM_RE = re.compile(r"(?<![\d.])(\d{1,2},?\d{3}\.\d{2}|\d{3}\.\d{2})(?![\d])")


def verify_indices_with_news(indices: dict, trade_date: str):
    """마감 기사 제목의 지수 종가로 교차검증. KRX 일별 지수 표가 마감 직후 최종값이 아닐 때가 있어,
    같은 종가를 적은 기사가 2건 이상이면 그 값을 우선 사용 (KRX 호출 없음, 구글 뉴스 1회)."""
    try:
        res = requests.get("https://news.google.com/rss/search",
                           params={"q": "코스피 코스닥 마감 when:1d", "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
                           headers={"User-Agent": "Mozilla/5.0 (compatible; KoreaMarketRadar/1.0)"}, timeout=15)
        if res.status_code != 200:
            log(f"지수 기사 교차검증: 뉴스 HTTP {res.status_code} → 건너뜀")
            return
        root = ET.fromstring(res.content)
    except Exception as e:
        log(f"지수 기사 교차검증: 뉴스 조회 실패 ({e}) → 건너뜀")
        return
    since = datetime.strptime(trade_date, "%Y-%m-%d").replace(hour=15, minute=30, tzinfo=KST)
    until = since + timedelta(hours=17, minutes=30)
    titles = []
    for it in root.iter("item"):
        try:
            pub = parsedate_to_datetime(it.findtext("pubDate")).astimezone(KST)
        except Exception:
            continue
        t = (it.findtext("title") or "").strip()
        if since <= pub <= until and CLOSE_WORDS.search(t):
            titles.append(t)
    for key, kw in INDEX_NEWS_KW.items():
        o = indices.get(key) or {}
        krx = float(o.get("price") or 0)
        if not krx or o.get("date") != trade_date:
            continue
        votes = {}
        for t in titles:
            pos = [m.start() for m in re.finditer("코스피|코스닥", t)]
            for i, st in enumerate(pos):
                if not t.startswith(kw, st):
                    continue
                seg = t[st: pos[i + 1] if i + 1 < len(pos) else len(t)]
                for m in NUM_RE.finditer(seg):
                    v = float(m.group(1).replace(",", ""))
                    if abs(v / krx - 1) <= 0.08:
                        votes[v] = votes.get(v, 0) + 1
                        break
        if not votes:
            log(f"{key}: 기사 교차검증 - 마감 기사 없음 (KRX {krx:,.2f} 사용)")
            continue
        best, n = max(votes.items(), key=lambda x: x[1])
        if abs(best - krx) < 0.005:
            log(f"{key}: 기사 교차검증 일치 ({krx:,.2f}, 기사 {n}건)")
            o["verified"] = "news"
            continue
        if n < 2:
            log(f"{key}: 기사 값 {best:,.2f} 1건뿐 → KRX {krx:,.2f} 유지")
            continue
        series = o.get("series") or []
        closes = [x["c"] for x in series]
        if len(closes) < 2:
            continue
        prev1 = closes[-2]
        prev5 = closes[-6] if len(closes) >= 6 else closes[0]
        o["price"] = round(best, 2)
        o["daily_pct"] = round((best - prev1) / prev1 * 100, 2)
        o["weekly_pct"] = round((best - prev5) / prev5 * 100, 2)
        series[-1]["c"] = round(best, 2)
        o["verified"] = "news"
        log(f"{key}: KRX 지수 {krx:,.2f}가 마감 기사({n}건) {best:,.2f}와 달라 기사 값 반영 → {o['daily_pct']:+.2f}%")


def fetch_sectors(start_date: str, prev: dict, cutoff: str) -> list:
    prev_map = {s.get("ticker"): s for s in (prev.get("sectors") or []) if isinstance(s, dict)}
    default = {"price": 0, "daily_pct": 0.0, "weekly_pct": 0.0, "date": None}
    sectors = []
    for item in ETF_LIST:
        metrics = get_price_metrics(safe_read(item["ticker"], start_date), cutoff)
        prev_item = prev_map.get(item["ticker"])
        prev_metrics = {k: prev_item.get(k) for k in ("price", "daily_pct", "weekly_pct", "date")} if prev_item else None
        sectors.append({**item, **keep_previous(metrics, prev_metrics, default)})
    return sectors


# ---------------------------------------------------------------------------
# 시장 전체 투자자 수급 (pykrx, KRX 로그인 필요)
# ---------------------------------------------------------------------------
def get_market_flow(market: str, cutoff: str):
    end_dt = now_kst().strftime("%Y%m%d")
    start_dt = (now_kst() - timedelta(days=14)).strftime("%Y%m%d")
    df = krx_call(f"{market} 수급", stock.get_market_trading_value_by_date if stock else None, start_dt, end_dt, market)
    df = cut(df, cutoff)
    if df is None or df.empty:
        return None
    cols = {"foreign": "외국인합계", "inst": "기관합계", "retail": "개인"}
    if any(c not in df.columns for c in cols.values()):
        log(f"{market} 수급: 예상 컬럼 없음 {list(df.columns)}")
        return None
    df5 = df.tail(5)
    out = {}
    for k, c in cols.items():
        out[f"daily_{k}"] = eok(df[c].iloc[-1])
        out[f"weekly_{k}"] = eok(df5[c].sum())
    out["date"] = df.index[-1].strftime("%Y-%m-%d")
    out["stale"] = False
    return out


def fetch_flow(prev: dict, cutoff: str) -> dict:
    prev_flow = prev.get("flow", {}) or {}
    return {key: keep_previous(get_market_flow(mk, cutoff), prev_flow.get(key), EMPTY_FLOW) for key, mk in MARKETS}


# ---------------------------------------------------------------------------
# 업종 분류 / 시장 폭 / 업종지수 / 업종별 수급 / 상위 종목
# ---------------------------------------------------------------------------
def norm(name: str) -> str:
    return re.sub(r"[\s·ㆍ,./()&-]", "", str(name))


def get_classification(trade_date: str, market: str):
    df = krx_call(f"{market} 업종분류", stock.get_market_sector_classifications if stock else None, ymd(trade_date), market)
    if df is None:
        return None
    if "종목코드" in df.columns:
        df = df.set_index("종목코드")
    df.index = df.index.astype(str)
    return df


def breadth_from(cls_df):
    if cls_df is None or "등락률" not in cls_df.columns:
        return None
    r = pd.to_numeric(cls_df["등락률"], errors="coerce").dropna()
    return {"up": int((r > 0).sum()), "down": int((r < 0).sum()), "flat": int((r == 0).sum()), "stale": False}


SECTOR_EXCLUDE = re.compile(r"(코스피|코스닥|KRX|200|150|100|50|대형|중형|소형|배당|레버리지|인버스|선물|변동성|커버드|ESG|TR|동일가중|우량|프리미어|글로벌|밸류|성장|가치)")


def get_sector_indices(market: str, dates: list, sector_names: set):
    """업종지수 등락률(일간·5일). dates: 오름차순 거래일 목록(YYYY-MM-DD)."""
    if len(dates) < 2:
        return None
    out = {}
    # KRX 기간등락률은 '시작일의 전일 종가' 대비 → 일간은 (당일~당일), 5일은 (5거래일 전~당일)
    for period, start in (("daily", dates[-1]), ("weekly", dates[-5] if len(dates) >= 5 else dates[0])):
        df = krx_call(f"{market} 업종지수({period})", stock.get_index_price_change if stock else None, ymd(start), ymd(dates[-1]), market)
        if df is None or "등락률" not in df.columns:
            return None
        df["등락률"] = pd.to_numeric(df["등락률"], errors="coerce")
        df["거래대금"] = pd.to_numeric(df.get("거래대금", 0), errors="coerce").fillna(0)
        if period == "daily":
            mk_name = "코스피" if market == "KOSPI" else "코스닥"
            if mk_name in df.index:
                log(f"{market} 업종지수 검증: {mk_name} 등락률 {df.loc[mk_name, '등락률']:.2f}%")
        rows = []
        for name, r in df.iterrows():
            n = norm(name)
            if sector_names:
                if n not in sector_names:
                    continue
            elif SECTOR_EXCLUDE.search(str(name)):
                continue
            if pd.isna(r["등락률"]):
                continue
            rows.append({"name": str(name), "pct": round(float(r["등락률"]), 2), "value": eok(r["거래대금"])})
        if not rows:
            log(f"{market} 업종지수({period}): 업종 매칭 0건 → 전체 지수명 {list(df.index)[:60]}")
            return None
        out[period] = sorted(rows, key=lambda x: x["pct"], reverse=True)
    out["stale"] = False
    return out


SECTOR_DRIVERS_N = 3  # 업종별 표시할 기여 종목 수


def attach_sector_drivers(si: dict, cls_df):
    """업종별 '오늘 지수를 움직인 종목': 업종 내 시총 비중 × 일간 등락률(=지수 기여도, %p) 상위.
    업종이 올랐으면 플러스 기여 상위, 내렸으면 마이너스 기여 상위. 추가 KRX 호출 없음."""
    if not si or cls_df is None or not {"업종명", "등락률", "시가총액"} <= set(cls_df.columns):
        return
    df = cls_df.copy()
    df["등락률"] = pd.to_numeric(df["등락률"], errors="coerce")
    df["시가총액"] = pd.to_numeric(df["시가총액"], errors="coerce")
    df = df.dropna(subset=["등락률", "시가총액"])
    df = df[df["시가총액"] > 0]
    nm = "종목명" if "종목명" in df.columns else None
    by_sector = {}
    for sec, g in df.groupby("업종명"):
        tot = g["시가총액"].sum()
        if tot <= 0:
            continue
        g = g.assign(w=g["시가총액"] / tot * 100, c=g["시가총액"] / tot * g["등락률"])
        by_sector[norm(sec)] = g
    for row in si.get("daily", []):
        g = by_sector.get(norm(row["name"]))
        if g is None:
            continue
        g = g.sort_values("c", ascending=row["pct"] < 0)
        g = g[g["c"] > 0] if row["pct"] >= 0 else g[g["c"] < 0]
        row["drivers"] = [{"name": str(r[nm]) if nm else str(code), "ret": round(float(r["등락률"]), 2),
                           "w": round(float(r["w"]), 1), "c": round(float(r["c"]), 2)}
                          for code, r in g.head(SECTOR_DRIVERS_N).iterrows()]
    # 주간 목록에도 같은 업종의 '오늘 기여' 종목을 붙여 둠(주간 종목 수익률은 없음)
    dmap = {r["name"]: r.get("drivers") for r in si.get("daily", [])}
    for row in si.get("weekly", []):
        if dmap.get(row["name"]):
            row["drivers"] = dmap[row["name"]]
            row["drivers_basis"] = "1D"


def get_net_purchases(market: str, start: str, end: str, investor: str):
    df = krx_call(
        f"{market} {investor} 순매수({start}~{end})",
        stock.get_market_net_purchases_of_equities if stock else None,
        ymd(start), ymd(end), market, investor,
    )
    if df is None or "순매수거래대금" not in df.columns:
        return None
    df.index = df.index.astype(str)
    df["순매수거래대금"] = pd.to_numeric(df["순매수거래대금"], errors="coerce").fillna(0)
    return df


STOCK_RET = {}  # 국내 종목 코드 → {name, ret(일간 등락률)}


def fetch_sector_block(dates: list, prev: dict):
    """업종지수·업종별 수급·상위 종목·시장 폭을 한 번에 수집."""
    trade = dates[-1]
    periods = {"daily": (trade, trade), "weekly": (dates[-5] if len(dates) >= 5 else dates[0], trade)}

    sector_indices, sector_flow, breadth = {}, {"daily": {}, "weekly": {}}, {}
    stock_rows = {"daily": {k: [] for k, _ in INVESTORS}, "weekly": {k: [] for k, _ in INVESTORS}}
    ok_flow = True

    for key, market in MARKETS:
        cls_df = get_classification(trade, market)
        breadth[key] = breadth_from(cls_df)
        if cls_df is not None and "등락률" in cls_df.columns:
            nm_col = "종목명" if "종목명" in cls_df.columns else None
            for code, r in cls_df.iterrows():
                v = _num(r["등락률"])
                if v == v:
                    STOCK_RET[str(code)] = {"name": str(r[nm_col]) if nm_col else str(code), "ret": round(v, 2)}
        sector_names = set(norm(x) for x in cls_df["업종명"].unique()) if cls_df is not None and "업종명" in cls_df.columns else set()
        sector_indices[key] = get_sector_indices(market, dates, sector_names)
        try:
            attach_sector_drivers(sector_indices[key], cls_df)
        except Exception as e:
            log(f"{market} 업종 기여 종목 계산 실패(무시): {type(e).__name__}: {e}")

        for period, (s, e) in periods.items():
            agg = {}
            per_stock = {}  # 업종 → 종목 → 투자자별 순매수(억)
            for inv_key, inv_name in INVESTORS:
                df = get_net_purchases(market, s, e, inv_name)
                if df is None:
                    ok_flow = False
                    continue
                if cls_df is not None and "업종명" in cls_df.columns:
                    df = df.join(cls_df[["업종명"]], how="left")
                else:
                    df["업종명"] = "기타"
                df["업종명"] = df["업종명"].fillna("기타")
                for sec, v in df.groupby("업종명")["순매수거래대금"].sum().items():
                    agg.setdefault(sec, {"sector": sec, "foreign": 0.0, "inst": 0.0, "retail": 0.0})[inv_key] = eok(v)
                name_col = "종목명" if "종목명" in df.columns else None
                for code, r in df.iterrows():
                    if r["순매수거래대금"] == 0:
                        continue
                    ps = per_stock.setdefault(str(r["업종명"]), {}).setdefault(code, {"name": str(r[name_col]) if name_col else code,
                                                                                      "foreign": 0, "inst": 0, "retail": 0})
                    ps[inv_key] = eok(r["순매수거래대금"])
                    stock_rows[period][inv_key].append({
                        "code": code,
                        "name": str(r[name_col]) if name_col else code,
                        "sector": str(r["업종명"]),
                        "market": market,
                        "value": eok(r["순매수거래대금"]),
                    })
            rows = list(agg.values())
            for r in rows:
                r["smart"] = r["foreign"] + r["inst"]  # 외국인+기관
                stocks = list((per_stock.get(r["sector"]) or {}).values())
                for st in stocks:
                    st["smart"] = st["foreign"] + st["inst"]
                tops = {}
                for k in ("smart", "foreign", "inst", "retail"):
                    sign = 1 if r[k] >= 0 else -1  # 업종이 순매수면 순매수 상위, 순매도면 순매도 상위
                    cand = sorted([x for x in stocks if x[k] * sign > 0], key=lambda x: x[k] * sign, reverse=True)[:SECTOR_DRIVERS_N]
                    tops[k] = [{"name": x["name"], "v": x[k]} for x in cand]
                r["drivers"] = tops
            sector_flow[period][key] = sorted(rows, key=lambda x: x["smart"], reverse=True)

    top_stocks = {}
    for period, by_inv in stock_rows.items():
        top_stocks[period] = {}
        for inv_key, rows in by_inv.items():
            rows = sorted(rows, key=lambda x: x["value"], reverse=True)
            top_stocks[period][inv_key] = {"buy": rows[:7], "sell": list(reversed(rows[-7:])) if rows else []}

    # 이전값 보존
    prev_si = prev.get("sector_indices") or {}
    prev_br = prev.get("breadth") or {}
    sector_indices = {k: keep_previous(sector_indices.get(k), prev_si.get(k), {"daily": [], "weekly": []}) for k, _ in MARKETS}
    breadth = {k: keep_previous(breadth.get(k), prev_br.get(k), {"up": 0, "down": 0, "flat": 0}) for k, _ in MARKETS}

    has_flow = any(sector_flow["daily"].get(k) for k, _ in MARKETS)
    if not has_flow or not ok_flow:
        prev_sf, prev_ts = prev.get("sector_flow"), prev.get("top_stocks")
        if not has_flow and prev_sf:
            sector_flow = dict(prev_sf, stale=True)
            top_stocks = dict(prev_ts or {}, stale=True)
        elif not has_flow:
            sector_flow = {"daily": {}, "weekly": {}, "stale": True}
            top_stocks = {"stale": True}
        else:
            sector_flow["partial"] = True  # 일부 투자자 수집 실패
    else:
        sector_flow["stale"] = False
        top_stocks["stale"] = False

    return sector_indices, sector_flow, top_stocks, breadth


# ---------------------------------------------------------------------------
# ACE ETF 라인업 흐름
# ---------------------------------------------------------------------------
ACE_PREFIX = "ACE "


def etf_names():
    """ETF 전종목 이름표 (pykrx 내부 싱글톤 - 실행당 1회만 조회)."""
    if KRX_STATE["down"]:
        return {}
    try:
        from pykrx.website.krx.etx.ticker import EtxTicker
        df = EtxTicker().df
        time.sleep(KRX_PAUSE)
        return df[df["시장"] == "ETF"]["종목명"].to_dict()
    except Exception as e:
        log(f"ETF 이름표 조회 실패: {type(e).__name__}: {e}")
        return {}


# 테마 분류: 위에서부터 먼저 맞는 테마 하나에만 배정 (ETF 이름 기준)
THEMES = [
    # 0) 전략형 인컴은 별도 (기초자산과 수익 구조가 다름)
    ("커버드콜(전략)", r"커버드콜"),
    # 1) 자산 성격이 분명한 것
    ("채권혼합", r"채권혼합|채혼합|국채혼합|TDF|TRF|자산배분|주식혼합|멀티에셋"),
    ("채권·금리·파킹", r"국고채|국채|특수채|금융채|은행채|여전채|공사채|전단채|물가채|스트립|채권|회사채|크레딧|금리|CD(?!MO)|KOFR|SOFR|머니마켓|단기|통안|하이일드|국공채|미국채|T-?Bill"),
    ("통화(달러)", r"달러선물|미국달러"),
    ("통화(엔)", r"엔선물|엔화"),
    ("리츠·부동산", r"리츠|부동산|REIT"),
    ("금·원자재", r"금현물|골드|금선물|금액티브|국제금|은액티브|은선물|팔라듐|원유|WTI|구리|원자재|금속|천연가스|농산물|콩선물|금채굴|자원생산|희토류|전략자원|탄소배출권|농업"),
    # 2) 지역 (이름에 지역이 있으면 지역 우선: 차이나테크 → 중국)
    ("중국·홍콩", r"중국|차이나|China|항셍|홍콩|과창판|CSI|심천|상해|창업판|BYD|샤오미"),
    ("일본·유럽", r"일본|TOPIX|니케이|Nikkei|유로|유럽|독일|프랑스|영국"),
    ("인도·신흥국", r"인도|베트남|인도네시아|멕시코|브라질|신흥|아세안|중남미|사우디|라틴|러시아|MSCI EM|EM선물"),
    ("미국 섹터", r"(미국|S&P|나스닥|NASDAQ).*(필수소비재|유틸리티|에너지|헬스케어|금융|산업재|소재|커뮤니케이션|임의소비재|경기소비재|소비재|은행|보험|정유)"),
    # 3) 산업·테마
    ("반도체", r"반도체|하이닉스|삼성전자|HBM|ASML|TSMC|MLCC|기판"),
    ("전력·원전·인프라", r"전력|원자력|원전|SMR|전선|그리드|인프라|데이터센터|송전"),
    ("2차전지", r"2차전지|이차전지|배터리|리튬|양극재"),
    ("자동차·모빌리티", r"자동차|모빌리티|로보택시|스마트카|전기차|수소차|(?<![A-Za-z])EV|UAM|드론"),
    ("친환경·에너지전환", r"친환경|그린|신재생|태양광|(?<!필)수소|클린에너지|기후변화|탄소|ESS|연료전지"),
    ("바이오·헬스케어", r"바이오|헬스케어|제약|빅파마|일라이릴리|비만|의료|헬스|치매|뇌질환|시니어"),
    ("조선·방산·우주", r"조선|방산|방위|우주|항공|스페이스|기계|중공업"),
    ("금융·밸류업", r"은행|금융|증권|보험|밸류업|지주|PBR|주주가치|주주환원"),
    ("미국 빅테크·성장", r"빅테크|FANG|엔비디아|테슬라|애플|마이크로소프트|구글|알파벳|아마존|메타(?!버스)|브로드컴|팔란티어|M7|매그니피센트|미국.*테크|테크.*미국"),
    ("AI·소프트웨어", r"(?<![A-Za-z])AI|인공지능|소프트웨어|클라우드|로봇|로보틱스|휴머노이드|양자|플랫폼|인터넷|자율주행|테크|5G|광통신|커머스|사이버보안|디지털"),
    ("배당·인컴", r"배당|인컴|월지급"),
    ("K컬처·소비", r"KPOP|K-POP|미디어|엔터|게임|화장품|뷰티|소비|음식료|푸드|여행|K컬처|컨텐츠|콘텐츠|럭셔리|웹툰|골프|저작권"),
    ("그룹주", r"^(?!.*(미국|(?<!코스닥)글로벌|아시아|선진국|월드)).*그룹"),
    ("국내 업종", r"^(?!.*(미국|(?<!코스닥)글로벌|아시아|선진국|월드)).*(200 ?(IT|건설|산업재|에너지화학|철강소재|중공업)|150IT|(?<![A-Za-z])IT(?![A-Za-z])|철강|건설|운송|에너지화학|경기방어|내수|수출|우선주|BBIG|제조업|소부장|CAPEX|뉴딜)"),
    ("미국 대표지수", r"S&P500|S&P 500|나스닥|NASDAQ|다우존스|러셀|미국\s?MSCI|미국대표|미국 대표|미국S&P"),
    ("미국 스타일·기타", r"미국"),
    ("국내 대표지수", r"^(?!.*(미국|(?<!코스닥)글로벌|아시아|선진국|월드)).*((?<![0-9])200|코스피|KOSPI|코스닥|KOSDAQ|KRX ?300|KRX100|KTOP|MSCI ?Korea|MSCI ?KOREA|코리아TOP|Top10동일|Top5|대형주|중형주|중소형|블루칩|우량)"),
    ("국내 스타일·팩터", r"^(?!.*(미국|(?<!코스닥)글로벌|아시아|선진국|월드)).*(가치|성장|모멘텀|로우볼|저변동|멀티팩터|퀄리티|ESG|동일가중|팩터|퀀트|혁신)"),
    ("글로벌 분산·기타", r"글로벌|선진국|토탈월드|월드|아시아|MSCI"),
]
THEME_RE = [(n, re.compile(rx, re.I)) for n, rx in THEMES]
LEV_INV = re.compile(r"레버리지|인버스|2X|곱버스|선물인버스|-1X|2배|3배", re.I)
# 전략형(롱숏·헤지·버퍼 등)은 기초 테마와 수익 구조가 달라 테마 비교에서 제외
STRATEGY = re.compile(r"롱|숏|헤지|버퍼|목표|양매도|변동성|VIX|절대수익|마켓뉴트럴|매니지드|스트래들|하방", re.I)
OTHER_BRANDS = re.compile(r"\b(KODEX|TIGER|SOL|RISE|KBSTAR|HANARO|PLUS|KIWOOM|KOSEF|ARIRANG|1Q|TIME|TIMEFOLIO|BNK|WON|마이티|파워|UNICORN|FOCUS|에셋플러스|히어로즈|VITA|DAISHIN343|TRUSTON|마이다스|HK|KCGI|BNK|하나)\b")


def theme_of(name: str):
    if LEV_INV.search(name) or STRATEGY.search(name):
        return None
    name = re.sub(r"(?i)ex-?China", "", name)  # '중국 제외' 상품이 중국 테마로 가지 않게
    for n, rx in THEME_RE:
        if rx.search(name):
            return n
    return None


def _num(v):
    try:
        return float(str(v).replace(",", "").strip() or "nan")
    except Exception:
        return float("nan")


def etf_snapshot(date: str):
    """ETF 전종목 시세(1회): 종가·1일 등락률·거래대금·순자산총액(AUM)."""
    from pykrx.website.krx.etx.core import 전종목시세_ETF
    df = krx_call(f"ETF 전종목 시세({date})", lambda d: 전종목시세_ETF().fetch(d), ymd(date))
    if df is None or "ISU_SRT_CD" not in df.columns:
        return None
    out = {}
    for _, r in df.iterrows():
        aum = _num(r.get("INVSTASST_NETASST_TOTAMT"))
        if not (aum > 0):
            aum = _num(r.get("MKTCAP"))  # 순자산 미제공 시 시가총액으로 대체(ETF는 거의 동일)
        out[str(r["ISU_SRT_CD"])] = {
            "d1": _num(r.get("FLUC_RT")),
            "price": _num(r.get("TDD_CLSPRC")),
            "value": _num(r.get("ACC_TRDVAL")),
            "aum": aum,
        }
    return out


def etf_period_change(start: str, end: str, label: str):
    df = krx_call(f"ETF 등락률({label})", stock.get_etf_price_change_by_ticker if stock else None, ymd(start), ymd(end))
    if df is None or "등락률" not in df.columns:
        return {}
    df.index = df.index.astype(str)
    return {c: _num(v) for c, v in df["등락률"].items()}


def r2(v):
    return None if v != v else round(float(v), 2)  # NaN → None


COMPARE_MIN_VALUE = 1  # 비교군은 당일 거래대금 1억 원 이상만
COMPARE_MIN_AUM = 100  # 비교군(타사)은 순자산 100억 원 이상만 (초소형 상품 노이즈 방지)


# 테마 기준지수: 국내는 KRX 지수(KRX·테마 시리즈), 해외는 대표 지수, 없으면 ETF 순자산 가중평균
THEME_BENCH = {
    "반도체": [("idx", r"^KRX 반도체$"), ("idx", r"반도체")],
    "2차전지": [("idx", r"2차전지 TOP ?10"), ("idx", r"2차전지")],
    "바이오·헬스케어": [("idx", r"^KRX 헬스케어$"), ("idx", r"헬스케어|바이오")],
    "조선·방산·우주": [("idx", r"조선"), ("idx", r"방산|방위"), ("idx", r"기계장비")],
    "금융·밸류업": [("idx", r"^KRX 은행$"), ("idx", r"밸류업"), ("idx", r"은행|금융")],
    "전력·원전·인프라": [("idx", r"원자력|원전"), ("idx", r"전력|유틸리티")],
    "자동차·모빌리티": [("idx", r"^KRX 자동차$"), ("idx", r"자동차|모빌리티")],
    "AI·소프트웨어": [("idx", r"인터넷|소프트웨어"), ("idx", r"정보기술|(?<![A-Za-z])AI")],
    "K컬처·소비": [("idx", r"K콘텐츠"), ("idx", r"미디어|엔터")],  # 경기소비재(자동차 등 포함)는 성격이 달라 제외
    "친환경·에너지전환": [("idx", r"기후변화 ?솔루션"), ("idx", r"기후변화|신재생|(?<!필)수소|탄소")],
    "배당·인컴": [("idx", r"고배당 ?50"), ("idx", r"고배당|배당")],
    "커버드콜(전략)": [("idx", r"커버드콜")],
    "국내 스타일·팩터": [("idx", r"모멘텀|로우볼|저변동|퀄리티|팩터")],
    "리츠·부동산": [("idx", r"리츠|부동산")],
    "국내 대표지수": [("kospi", None)],
    "통화(달러)": [("usdkrw", None)],
    "미국 대표지수": [("fdr", ["US500", "^GSPC"])],
    "미국 빅테크·성장": [("fdr", ["^NDX", "IXIC"])],
    "중국·홍콩": [("fdr", ["^HSI", "HSI"])],
    "일본·유럽": [("fdr", ["^N225", "N225"])],
    "인도·신흥국": [("fdr", ["^NSEI", "NIFTY50"])],
    "금·원자재": [("fdr", ["GC=F", "GC"])],
}
# 해외 지수는 국내 장 마감 이후 끝나는 시장이 많아 기준 거래일 '전날'까지 (일본은 당일 마감 반영)
FDR_SAME_DAY = {"JP225", "^N225", "N225"}


def fetch_index_change_table(dates: list) -> dict:
    """KRX·테마 시리즈 지수 전체 등락률(1D/1W/1M). KRX 6회 호출. 지수명 목록은 history/index_names.json에 기록."""
    trade = dates[-1]
    starts = {"d1": trade, "w1": dates[-5] if len(dates) >= 5 else dates[0], "m1": dates[-20] if len(dates) >= 20 else dates[0]}
    table = {}
    for market in ("KRX", "테마"):
        for k, st in starts.items():
            df = krx_call(f"{market} 지수 등락률({k})", stock.get_index_price_change if stock else None, ymd(st), ymd(trade), market)
            if df is None or "등락률" not in df.columns:
                continue
            for name, v in df["등락률"].items():
                table.setdefault(str(name), {})[k] = r2(_num(v))
    try:
        os.makedirs(HISTORY_DIR, exist_ok=True)
        with open(os.path.join(HISTORY_DIR, "index_names.json"), "w", encoding="utf-8") as f:
            json.dump(sorted(table.keys()), f, ensure_ascii=False, indent=1)
    except Exception:
        pass
    log(f"KRX·테마 지수 {len(table)}개 등락률 확보")
    return table


def fdr_bench(symbols, trade_date: str, start_date: str):
    for sym in symbols:
        df = safe_read(sym, start_date, retries=1)
        if df is None:
            continue
        cutoff = trade_date if sym in FDR_SAME_DAY else (pd.Timestamp(trade_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        df = cut(df, cutoff)
        if df is None or len(df) < 6:
            continue
        c = df["Close"].astype(float)
        last = c.iloc[-1]
        def chg(n):
            return r2((last / c.iloc[-1 - n] - 1) * 100) if len(c) > n else None
        return {"name": sym, "d1": chg(1), "w1": chg(5), "m1": chg(20) if len(c) > 20 else r2((last / c.iloc[0] - 1) * 100),
                "date": df.index[-1].strftime("%Y-%m-%d"), "src": "해외 지수"}
    return None


FDR_LABEL = {"US500": "S&P 500", "^GSPC": "S&P 500", "^NDX": "나스닥 100", "IXIC": "나스닥 종합", "HK50": "항셍", "^HSI": "항셍", "HSI": "항셍",
             "JP225": "닛케이 225", "^N225": "닛케이 225", "N225": "닛케이 225", "^NSEI": "인도 Nifty 50", "NIFTY50": "인도 Nifty 50",
             "GC=F": "금 선물", "GC": "금 선물"}


def build_bench_map(dates: list, idx_table: dict, indices: dict, start_date: str) -> dict:
    out = {}
    trade = dates[-1]
    for theme, cands in THEME_BENCH.items():
        for kind, arg in cands:
            b = None
            if kind == "idx" and idx_table:
                rx = re.compile(arg)
                names = sorted([n for n in idx_table if rx.search(n) and idx_table[n].get("d1") is not None], key=len)
                if names:
                    n = names[0]
                    b = dict(idx_table[n], name=n, src="KRX 지수", date=trade)
            elif kind == "kospi":
                k = indices.get("kospi") or {}
                ser = k.get("series") or []
                m1 = r2((ser[-1]["c"] / ser[0]["c"] - 1) * 100) if len(ser) >= 2 else None
                b = {"name": "코스피", "d1": k.get("daily_pct"), "w1": k.get("weekly_pct"), "m1": m1, "src": "KRX 지수", "date": trade}
            elif kind == "usdkrw":
                k = indices.get("usdkrw") or {}
                ser = k.get("series") or []
                m1 = r2((ser[-1]["c"] / ser[0]["c"] - 1) * 100) if len(ser) >= 2 else None
                b = {"name": "원/달러 환율", "d1": k.get("daily_pct"), "w1": k.get("weekly_pct"), "m1": m1, "src": "환율", "date": k.get("date")}
            elif kind == "fdr":
                b = fdr_bench(arg, trade, start_date)
                if b:
                    b["name"] = FDR_LABEL.get(b["name"], b["name"])
            if b and b.get("d1") is not None:
                out[theme] = b
                break
    log("테마 기준지수: " + ", ".join(f"{t}={b['name']}" for t, b in out.items()))
    return out


def aum_weighted(g: list, key: str):
    num_, den = 0.0, 0.0
    for x in g:
        v, w = x.get(key), x.get("aum") or 0
        if v is not None and w > 0:
            num_ += v * w
            den += w
    return r2(num_ / den) if den else None


# ---- 테마를 움직인 종목 (국내 주식 테마, 대표 ETF 구성종목 기반) ----
DOMESTIC_THEMES = {"반도체", "2차전지", "바이오·헬스케어", "조선·방산·우주", "금융·밸류업", "전력·원전·인프라",
                   "자동차·모빌리티", "AI·소프트웨어", "K컬처·소비", "친환경·에너지전환", "그룹주", "국내 업종",
                   "국내 대표지수", "배당·인컴", "국내 스타일·팩터"}
FOREIGN_NAME = re.compile(r"미국|글로벌|차이나|중국|일본|인도|S&P|나스닥|채권|커버드콜|국채|선진국|아시아|유럽|대만|베트남")
PDF_PATH = "history/pdf_cache.json"
PDF_PER_RUN = 12      # 하루 최대 구성종목(PDF) 조회 수
PDF_MAX_AGE = 7       # 구성종목 캐시 유효기간(일)
REPS_PER_THEME = 2


def theme_reps(rows: list) -> dict:
    groups = {}
    for r in rows:
        t = theme_of(r["name"])
        if t in DOMESTIC_THEMES and not FOREIGN_NAME.search(r["name"]) and (r.get("aum") or 0) > 0:
            groups.setdefault(t, []).append(r)
    return {t: [x["code"] for x in sorted(g, key=lambda x: x["aum"], reverse=True)[:REPS_PER_THEME]] for t, g in groups.items()}


def refresh_pdf_cache(reps: dict, trade: str, allow_fetch: bool) -> dict:
    try:
        with open(PDF_PATH, encoding="utf-8") as f:
            cache = json.load(f)
    except Exception:
        cache = {}
    if not allow_fetch or stock is None:
        return cache
    need = []
    for codes in reps.values():
        for rank, c in enumerate(codes):
            ent = cache.get(c)
            age = 999 if not ent else (pd.Timestamp(trade) - pd.Timestamp(ent.get("date", "2000-01-01"))).days
            if age >= PDF_MAX_AGE:
                need.append((rank, -age, c))  # 테마별 1순위 ETF부터 → 모든 테마가 빨리 채워지게
    seen, ordered = set(), []
    for _, _, c in sorted(need):
        if c not in seen:
            seen.add(c)
            ordered.append(c)
    need = ordered[:PDF_PER_RUN]
    got = 0
    for c in need:
        df = krx_call(f"PDF {c}", stock.get_etf_portfolio_deposit_file, c, ymd(trade), retries=1)
        if df is None or "비중" not in df.columns:
            continue
        df = df.copy()
        df["비중"] = pd.to_numeric(df["비중"], errors="coerce").fillna(0)
        df = df[df["비중"] > 0].sort_values("비중", ascending=False).head(15)
        cache[c] = {"date": trade, "h": [[str(t), round(float(w), 2)] for t, w in df["비중"].items()]}
        got += 1
    if got:
        os.makedirs(HISTORY_DIR, exist_ok=True)
        atomic_write_json(PDF_PATH, cache)
    log(f"구성종목(PDF) 갱신 {got}/{len(need)}건 (캐시 {len(cache)}개 ETF)")
    return cache


def theme_drivers(reps: dict, cache: dict, names: dict) -> dict:
    out = {}
    for t, codes in reps.items():
        have = [c for c in codes if c in cache and cache[c].get("h")]
        if not have or not STOCK_RET:
            continue
        wsum, cnt = {}, {}
        for c in have:
            for tk, w in cache[c]["h"]:
                wsum[tk] = wsum.get(tk, 0) + w
                cnt[tk] = cnt.get(tk, 0) + 1
        n = len(have)
        common = [tk for tk in wsum if cnt[tk] >= 2] if n >= 2 else list(wsum)
        if len(common) < 3:
            common = list(wsum)
        rows = []
        for tk in common:
            sr = STOCK_RET.get(tk)
            if not sr:
                continue
            w = wsum[tk] / n
            rows.append({"name": sr["name"], "w": round(w, 1), "ret": sr["ret"], "c": round(w * sr["ret"] / 100, 2), "n": cnt[tk]})
        if not rows:
            continue
        rows.sort(key=lambda x: x["c"])
        out[t] = {
            "reps": [names.get(c, c) for c in have],
            "common": n >= 2,
            "up": [x for x in sorted(rows, key=lambda x: x["c"], reverse=True) if x["c"] > 0][:3],
            "down": [x for x in rows if x["c"] < 0][:3],
        }
    return out


def build_themes(rows: list, key: str, bench: dict = None, drivers: dict = None) -> list:
    bench, drivers = bench or {}, drivers or {}
    groups = {}
    for r in rows:
        t = theme_of(r["name"])
        if t and r.get(key) is not None:
            groups.setdefault(t, []).append(r)
    out = []
    for t, _ in THEMES:
        g = groups.get(t) or []
        if len(g) < 2:
            continue
        b = bench.get(t)
        if not b or b.get(key) is None:
            # 맞는 지수가 없는 테마: 테마 안 수익률 1위 ETF (AUM 100억 이상, 없으면 전체 중)
            pool = [x for x in g if (x.get("aum") or 0) >= COMPARE_MIN_AUM] or g
            top = max(pool, key=lambda x: x[key])
            b = {"name": top["name"], "code": top["code"], "src": "상위 ETF",
                 "d1": top.get("d1"), "w1": top.get("w1"), "m1": top.get("m1")}
        ace = [x for x in g if x["name"].startswith(ACE_PREFIX)]
        others = [x for x in g if not x["name"].startswith(ACE_PREFIX) and x.get("aum", 0) >= COMPARE_MIN_AUM
                  and (x.get("value") or 0) >= COMPARE_MIN_VALUE]  # 거래가 거의 없는 상품은 종가가 기초자산과 어긋나 제외
        out.append({
            "theme": t,
            "bench": b,
            "median": b.get(key),  # (호환) 테마 대표 수익률 = 기준지수
            "count": len(g),
            "ace_count": len(ace),
            "ace_median": round(float(pd.Series([x[key] for x in ace]).median()), 2) if ace else None,
            "ace": sorted(ace, key=lambda x: x[key], reverse=True)[:8],
            "others": sorted(others, key=lambda x: x[key], reverse=True)[:3],
            "drivers": drivers.get(t),
        })
    return sorted(out, key=lambda x: (x["median"] is None, -(x["median"] or 0)))


def fetch_etf_block(dates: list, indices: dict = None, start_date: str = None, allow_pdf: bool = False):
    """ETF 데이터: 전종목 시세 1회 + 기간 등락률 2회(1주·1개월) = KRX 3회 (+이름표)."""
    names = etf_names()
    if not names:
        return None, None
    trade = dates[-1]
    snap = etf_snapshot(trade)
    if not snap:
        return None, None
    w1 = etf_period_change(dates[-5] if len(dates) >= 5 else dates[0], trade, "1주")
    m1 = etf_period_change(dates[-20] if len(dates) >= 20 else dates[0], trade, "1개월")
    rows = []
    for code, v in snap.items():
        if code not in names or v["d1"] != v["d1"]:
            continue
        rows.append({
            "code": code, "name": str(names[code]),
            "d1": r2(v["d1"]), "w1": r2(w1.get(code, float("nan"))), "m1": r2(m1.get(code, float("nan"))),
            "price": int(v["price"]) if v["price"] == v["price"] else 0,
            "value": eok(v["value"]) if v["value"] == v["value"] else 0,
            "aum": eok(v["aum"]) if v["aum"] == v["aum"] else 0,
        })
    try:
        tmap = {}
        for r in rows:
            t = theme_of(r["name"]) or ("레버리지·인버스(제외)" if LEV_INV.search(r["name"]) else "전략형(제외)" if STRATEGY.search(r["name"]) else "미분류")
            tmap.setdefault(t, []).append(r["name"])
        os.makedirs(HISTORY_DIR, exist_ok=True)
        with open(os.path.join(HISTORY_DIR, "theme_map.json"), "w", encoding="utf-8") as f:
            json.dump({k: sorted(v) for k, v in sorted(tmap.items())}, f, ensure_ascii=False, indent=1)
    except Exception as e:
        log(f"테마 분류표 저장 실패(무시): {e}")
    k200 = next((x for x in rows if x["code"] == "069500"), None)
    if k200:
        log(f"ETF 검증: KODEX 200 1D {k200['d1']}% / 1W {k200['w1']}% / 1M {k200['m1']}% / 순자산 {k200['aum']:,}억")

    arows = [x for x in rows if x["name"].startswith(ACE_PREFIX)]
    if not arows:
        return None, None
    ace = {"count": len(arows)}
    themes = {}
    try:
        idx_table = fetch_index_change_table(dates)
        bench = build_bench_map(dates, idx_table, indices or {}, start_date or dates[0])
    except Exception as e:
        log(f"테마 기준지수 실패(ETF 가중으로 대체): {type(e).__name__}: {e}")
        bench = {}
    try:
        reps = theme_reps(rows)
        cache = refresh_pdf_cache(reps, dates[-1], allow_pdf)
        drivers = theme_drivers(reps, cache, {r["code"]: r["name"] for r in rows})
    except Exception as e:
        log(f"테마 기여 종목 실패(무시): {type(e).__name__}: {e}")
        drivers = {}
    themes["picks_pool"] = [dict(x, theme=theme_of(x["name"]) or "기타") for x in sorted(arows, key=lambda x: x["aum"], reverse=True)
                            if (x.get("aum") or 0) >= 300][:40]
    for period, key in (("daily", "d1"), ("weekly", "w1")):
        themes[period] = build_themes(rows, key, bench, drivers)
        valid = [x for x in arows if x.get(key) is not None]
        by = sorted(valid, key=lambda x: x[key], reverse=True)
        sr = pd.Series([x[key] for x in valid])
        ace[period] = {
            "up": int((sr > 0).sum()), "down": int((sr < 0).sum()), "flat": int((sr == 0).sum()),
            "median": round(float(sr.median()), 2),
            "top": [dict(x, pct=x[key]) for x in by[:8]],
            "bottom": [dict(x, pct=x[key]) for x in by[-8:][::-1]],
            "value_top": [dict(x, pct=x[key]) for x in sorted(valid, key=lambda x: x["value"], reverse=True)[:8]],
            "aum_top": [dict(x, pct=x[key]) for x in sorted(valid, key=lambda x: x["aum"], reverse=True)[:8]],
        }
    ace["aum_total"] = int(sum(x["aum"] for x in arows))
    ace["stale"] = False
    themes["stale"] = False
    return ace, themes


# ---------------------------------------------------------------------------
# 관련 기사 (구글 뉴스 RSS)
# ---------------------------------------------------------------------------
NEWS_MUST = re.compile(r"(코스피|코스닥|증시|국내\s?증시|외국인|기관|마감)")
NEWS_EXCLUDE = re.compile(r"(베트남|미국\s?증시|뉴욕|일본|중국|홍콩|코인|비트코인)")


def fetch_news(prev: dict, trade_date: str):
    """기준 거래일 장 시작(08:00) ~ 다음 거래일 개장(09:00) 사이 기사만 사용."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; KoreaMarketRadar/1.0)"}
    items, seen = [], set()
    since = datetime.strptime(trade_date, "%Y-%m-%d").replace(hour=8, tzinfo=KST)
    today = now_kst()
    until = today.replace(hour=9, minute=0, second=0, microsecond=0) if today.strftime("%Y-%m-%d") > trade_date else today
    for qi, q in enumerate(NEWS_QUERIES):
        if qi:
            time.sleep(NEWS_PAUSE)
        try:
            res = None
            for attempt in range(1, 3):
                res = requests.get(
                    "https://news.google.com/rss/search",
                    params={"q": f"{q} when:2d", "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
                    headers=headers, timeout=15,
                )
                if res.status_code == 200:
                    break
                log(f"뉴스 검색({q}) HTTP {res.status_code} (시도 {attempt}/2)")
                time.sleep(NEWS_PAUSE * 2 * attempt)
            if res is None or res.status_code != 200:
                continue
            root = ET.fromstring(res.content)
            for it in root.iter("item"):
                title = (it.findtext("title") or "").strip()
                link = (it.findtext("link") or "").strip()
                source = (it.findtext("source") or "").strip()
                pub = it.findtext("pubDate")
                try:
                    pub_dt = parsedate_to_datetime(pub).astimezone(KST) if pub else None
                except Exception:
                    pub_dt = None
                if pub_dt and (pub_dt < since or pub_dt > until):
                    continue
                if source and title.endswith(f" - {source}"):
                    title = title[: -len(f" - {source}")]
                if not NEWS_MUST.search(title) or NEWS_EXCLUDE.search(title):
                    continue
                key = norm(title)[:40]
                if not title or key in seen:
                    continue
                seen.add(key)
                items.append({
                    "title": title, "source": source, "link": link,
                    "published": pub_dt.strftime("%m-%d %H:%M") if pub_dt else "",
                    "_ts": pub_dt.timestamp() if pub_dt else 0,
                })
        except Exception as e:
            log(f"뉴스 수집 오류({q}): {type(e).__name__}")
    if not items:
        old = prev.get("news")
        return {"items": (old or {}).get("items", []) if isinstance(old, dict) else [], "stale": True}
    items = sorted(items, key=lambda x: x["_ts"], reverse=True)[:NEWS_MAX]
    resolve_news_links(items)
    for i, it in enumerate(items, 1):
        it["id"] = i
        it.pop("_ts", None)
    return {"items": items, "stale": False}


def resolve_news_links(items):
    """구글 뉴스 중간 주소 → 언론사 원문 주소로 변환.
    실패 시 브라우저에서 열리는 /articles/ 형식으로라도 바꿔 둠."""
    for it in items:
        it["link"] = it["link"].replace("news.google.com/rss/articles/", "news.google.com/articles/")
    try:
        from googlenewsdecoder import gnewsdecoder
    except Exception:
        log("googlenewsdecoder 미설치 → 구글 뉴스 주소 유지")
        return
    urls = [it["link"] for it in items]
    try:
        results = gnewsdecoder(urls, interval=1, timeout=15)  # 기사당 1초 간격, 변환 요청은 1회
    except Exception as e:
        log(f"기사 원문 주소 변환 실패(구글 주소 유지): {type(e).__name__}")
        return
    ok = 0
    for it, r in zip(items, results or []):
        if isinstance(r, dict) and r.get("success") and str(r.get("decoded_url", "")).startswith("http"):
            it["link"] = r["decoded_url"]
            ok += 1
    log(f"기사 원문 주소 변환 {ok}/{len(items)}건")


# ---------------------------------------------------------------------------
# 시황 브리핑
# ---------------------------------------------------------------------------
def fmt_eok(v) -> str:
    return f"{v:+,.0f}억"


def top_n(rows, n, key="pct", reverse=True):
    return sorted(rows or [], key=lambda x: x.get(key, 0), reverse=reverse)[:n]


BRIEF_SECTIONS = [
    ("market", "지수·매크로"),
    ("flow", "수급"),
    ("sector", "업종"),
    ("stock", "종목"),
    ("issue", "이슈·뉴스"),
    ("ace", "ACE ETF"),
]


PICK_SKIP_THEMES = {"채권혼합", "기타", "커버드콜(전략)"}


def picks_pool(p: dict) -> list:
    return ((p.get("themes") or {}).get("picks_pool")) or []


def rule_picks(p: dict, n: int = 6) -> list:
    """규칙 기반 '오늘의 ACE ETF': 강세 테마의 ACE 대표 상품 + 시장 상황별 방어/코어 상품, 테마 중복 없이."""
    pool = picks_pool(p)
    if not pool:
        return []
    by_theme = {}
    for x in pool:  # pool은 AUM 큰 순
        by_theme.setdefault(x["theme"], x)
    themes = (p.get("themes") or {}).get("daily") or []
    kospi = (p.get("indices") or {}).get("kospi", {}).get("daily_pct") or 0
    picks, used = [], set()

    def add(x, point):
        if x and x["code"] not in {y["code"] for y in picks} and x["theme"] not in used:
            picks.append(dict(x, point=point))
            used.add(x["theme"])

    for t in themes:  # 기준지수 강세 순
        if len(picks) >= 3:
            break
        if t["theme"] in PICK_SKIP_THEMES or t["theme"] not in by_theme:
            continue
        b = t.get("bench") or {}
        basis = f"테마 1위 {b.get('name', '-')}" if b.get("src") == "상위 ETF" else b.get("name", "-")
        add(by_theme[t["theme"]], f"{t['theme']} 상대 강세 ({basis} {b.get('d1') or 0:+.2f}%)")
    defensive = ["채권·금리·파킹", "금·원자재", "배당·인컴"] if kospi < 0 else ["국내 대표지수", "미국 대표지수", "반도체"]
    for th in defensive + ["미국 대표지수", "미국 빅테크·성장", "국내 대표지수", "배당·인컴", "금·원자재"]:
        if len(picks) >= n:
            break
        x = by_theme.get(th)
        if x:
            add(x, f"{th} 대표 ACE 상품 · AUM {x['aum']:,}억, 1M {x.get('m1') or 0:+.2f}%")
    for th, x in by_theme.items():  # 그래도 모자라면 AUM 큰 테마 대표로 채움
        if len(picks) >= n:
            break
        if th not in PICK_SKIP_THEMES:
            add(x, f"{th} 대표 ACE 상품 · AUM {x['aum']:,}억")
    return picks[:n]


def build_rule_briefing(p: dict) -> dict:
    I, F = p["indices"], p["flow"]
    out = {}
    for period in ("daily", "weekly"):
        pk = f"{period}_pct"
        k, q, fx, sox = I["kospi"], I["kosdaq"], I["usdkrw"], I["sox"]
        fk, fq = F["kospi"], F["kosdaq"]
        mood = "약세" if k[pk] < -0.5 else ("강세" if k[pk] > 0.5 else "보합")
        si = ((p.get("sector_indices") or {}).get("kospi") or {}).get(period) or []
        sf = ((p.get("sector_flow") or {}).get(period) or {}).get("kospi") or []
        ts = (p.get("top_stocks") or {}).get(period) or {}
        b = (p.get("breadth") or {}).get("kospi") or {}

        lead = None
        if not fk.get("stale"):
            lead = max(INVESTORS, key=lambda x: abs(fk.get(f"{period}_{x[0]}", 0)))[0]
        headline = f"코스피 {k[pk]:+.2f}% {mood}" + (f", {INVESTOR_LABEL[lead]} {fmt_eok(fk[f'{period}_{lead}'])}이 방향 결정" if lead else "")

        summary = [f"코스피 {k['price']:,.2f}pt({k[pk]:+.2f}%), 코스닥 {q[pk]:+.2f}%로 {mood} 마감"]
        if lead:
            summary.append(f"수급 주체는 {INVESTOR_LABEL[lead]}({fmt_eok(fk[f'{period}_{lead}'])})")
        if si:
            summary.append(f"업종은 {si[0]['name']}({si[0]['pct']:+.2f}%) 강세, {si[-1]['name']}({si[-1]['pct']:+.2f}%) 약세")

        sec = {key: [] for key, _ in BRIEF_SECTIONS}
        sec["market"] = [
            f"지수: 코스피 {k['price']:,.2f}pt({k[pk]:+.2f}%), 코스닥 {q['price']:,.2f}pt({q[pk]:+.2f}%)",
            f"환율: 원/달러 {fx['price']:,.1f}원({fx[pk]:+.2f}%)"
            + (f", 전일 미 반도체 {'약세' if sox[pk] < 0 else '강세'} 영향 반영" if abs(sox[pk]) >= 1 else ""),
        ]
        if period == "daily" and b.get("up") is not None and not b.get("stale"):
            sec["market"].append(f"시장 폭: 코스피 상승 {b['up']} / 하락 {b['down']} / 보합 {b['flat']} 종목")
        if not fk.get("stale"):
            sec["flow"].append("코스피: " + ", ".join(f"{INVESTOR_LABEL[x]} {fmt_eok(fk[f'{period}_{x}'])}" for x, _ in INVESTORS))
        if not fq.get("stale"):
            sec["flow"].append("코스닥: " + ", ".join(f"{INVESTOR_LABEL[x]} {fmt_eok(fq[f'{period}_{x}'])}" for x, _ in INVESTORS))
        if sf:
            sec["flow"].append("외+기 매수 업종: " + ", ".join(f"{r['sector']} {fmt_eok(r['smart'])}" for r in sf[:3]))
            sec["flow"].append("외+기 매도 업종: " + ", ".join(f"{r['sector']} {fmt_eok(r['smart'])}" for r in sf[-3:][::-1]))
        if si:
            sec["sector"].append("강세: " + ", ".join(f"{r['name']} {r['pct']:+.2f}%" for r in si[:3]))
            sec["sector"].append("약세: " + ", ".join(f"{r['name']} {r['pct']:+.2f}%" for r in si[-3:][::-1]))
        fb = (ts.get("foreign") or {})
        if fb.get("buy"):
            sec["stock"].append("외국인 순매수: " + ", ".join(f"{x['name']} {fmt_eok(x['value'])}" for x in fb["buy"][:3]))
        if fb.get("sell"):
            sec["stock"].append("외국인 순매도: " + ", ".join(f"{x['name']} {fmt_eok(x['value'])}" for x in fb["sell"][:3]))
        news = (p.get("news") or {}).get("items") or []
        sec["issue"] = [f"기사 {n['id']}: {n['title']} [{n['id']}]" for n in news[:3]]
        ace = (p.get("ace") or {}).get(period) or {}
        if ace.get("top"):
            sec["ace"].append(f"등락 분포: ACE ETF {p['ace'].get('count', 0)}종 중 상승 {ace['up']} / 하락 {ace['down']}, 중간값 {ace['median']:+.2f}%")
            sec["ace"].append("상승 상위: " + ", ".join(f"{x['name']} {x['pct']:+.2f}%" for x in ace["top"][:3]))
            sec["ace"].append("거래대금 상위: " + ", ".join(f"{x['name']} {x['value']:,}억" for x in ace["value_top"][:3]))

        strategy = []
        if k[pk] < -2 or (not fk.get("stale") and fk.get(f"{period}_foreign", 0) < 0 and fx[pk] > 0.3):
            strategy.append("외국인 이탈·환율 부담 구간 → 추격 매수 자제, 낙폭 과대 업종은 분할 접근")
        if si:
            strategy.append(f"상대 강세 업종({si[0]['name']}) 중심 순환매 지속 여부 확인")
        strategy.append("다음 거래일 외국인 수급 방향과 반도체 업종 반등 여부 점검")
        gists = {
            "market": f"양대 지수 {mood}" + (f", 하락 종목 우세" if b.get("down", 0) > b.get("up", 0) else ""),
            "flow": (f"{INVESTOR_LABEL[lead]}이 방향 결정" if lead else "수급 데이터 확인 중"),
            "sector": (f"{si[0]['name']} 강세, {si[-1]['name']} 약세" if si else ""),
            "stock": "외국인 매매 상위 종목",
            "issue": "주요 마감 기사",
            "ace": "ACE 라인업 마감 흐름",
        }
        out[period] = {
            "headline": headline,
            "summary": summary,
            "sections": [{"key": key, "title": t, "gist": gists.get(key, ""), "items": [to_kv(x) for x in sec[key]]}
                         for key, t in BRIEF_SECTIONS if sec[key]],
            "strategy": strategy,
        }
    out["source"] = "rule"
    out["ace_picks"] = rule_picks(p)
    return out


# ---------------------------------------------------------------------------
# 오늘의 지표: 후보 중 '평소 대비' 가장 이례적으로 움직인 지표 선정 (FinanceDataReader, KRX 호출 없음)
# ---------------------------------------------------------------------------
SPOT_CANDS = [
    # key, 표시명, FDR 심볼 후보, 단위(pct=등락률 / bp=금리 변화), 같은날 마감 여부
    ("sox", "필라델피아 반도체", ["^SOX", "SOX"], "pct", False),
    ("us10y", "미국 10년물 금리", ["^TNX", "US10YT"], "bp", False),
    ("dxy", "달러인덱스", ["DX-Y.NYB", "DXY"], "pct", False),
    ("wti", "WTI 유가", ["CL=F"], "pct", False),
    ("gold", "금", ["GC=F"], "pct", False),
    ("ndx", "나스닥 100", ["^NDX", "IXIC"], "pct", False),
    ("nikkei", "닛케이 225", ["^N225", "N225"], "pct", True),
]


def spot_metrics(key, label, syms, unit, same_day, trade_date, start_date):
    for sym in syms:
        df = safe_read(sym, start_date, retries=1)
        if df is None:
            continue
        cutoff = trade_date if same_day else (pd.Timestamp(trade_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        df = cut(df, cutoff)
        if df is None or len(df) < 12:
            continue
        c = df["Close"].astype(float)
        if unit == "bp" and c.iloc[-1] > 20:  # 일부 소스는 금리 ×10 (예: 42.5 = 4.25%)
            c = c / 10
        ch = (c.diff() * 100) if unit == "bp" else (c.pct_change() * 100)
        ch = ch.dropna()
        today, hist = float(ch.iloc[-1]), ch.iloc[-21:-1]
        sd = float(hist.std()) if len(hist) >= 5 else 0.0
        z = abs(today) / sd if sd > 0 else 0.0
        return {"key": key, "label": label, "symbol": sym, "unit": unit,
                "value": round(float(c.iloc[-1]), 3 if unit == "bp" else 2),
                "chg": round(today, 1 if unit == "bp" else 2), "z": round(z, 1),
                "date": df.index[-1].strftime("%Y-%m-%d"),
                "series": [{"d": i.strftime("%m-%d"), "c": round(float(v), 3)} for i, v in c.tail(20).items()]}
    return None


def fetch_spotlight(trade_date: str, start_date: str, prev: dict):
    items = []
    for cand in SPOT_CANDS:
        try:
            m = spot_metrics(*cand, trade_date, start_date)
            if m:
                items.append(m)
        except Exception as e:
            log(f"오늘의 지표 {cand[1]} 실패(무시): {type(e).__name__}")
    if not items:
        old = prev.get("spotlight")
        return dict(old, stale=True) if old else None
    items.sort(key=lambda x: x["z"], reverse=True)
    pick = items[0]
    log("오늘의 지표: " + ", ".join(f"{x['label']} {x['chg']:+}{'bp' if x['unit'] == 'bp' else '%'}(z{x['z']})" for x in items))
    return {"pick": pick["key"], "items": items, "stale": False}


def briefing_input(p: dict) -> dict:
    def slim_si(m):
        si = (p.get("sector_indices") or {}).get(m) or {}
        return {per: {"top": top_n(si.get(per), 5), "bottom": top_n(si.get(per), 5, reverse=False)} for per in ("daily", "weekly")}

    def slim_sf(m):
        sf = p.get("sector_flow") or {}
        out = {}
        for per in ("daily", "weekly"):
            rows = (sf.get(per) or {}).get(m) or []
            out[per] = {"buy": rows[:5], "sell": rows[-5:][::-1]}
        return out

    ts = p.get("top_stocks") or {}
    slim_ts = {
        per: {inv: {"buy": [(x["name"], x["value"]) for x in (ts.get(per) or {}).get(inv, {}).get("buy", [])[:4]],
                    "sell": [(x["name"], x["value"]) for x in (ts.get(per) or {}).get(inv, {}).get("sell", [])[:4]]}
              for inv, _ in INVESTORS}
        for per in ("daily", "weekly")
    }
    idx = {k: {kk: v.get(kk) for kk in ("price", "daily_pct", "weekly_pct", "stale")} for k, v in p["indices"].items()}
    return {
        "trade_date": p.get("trade_date"),
        "indices": idx,
        "spotlight_overnight": [{k: x.get(k) for k in ("label", "chg", "unit", "z", "date")}
                                for x in ((p.get("spotlight") or {}).get("items") or [])],
        "flow_eok": p["flow"],
        "breadth": p.get("breadth"),
        "sector_index_kospi": slim_si("kospi"),
        "sector_index_kosdaq": slim_si("kosdaq"),
        "sector_flow_kospi_eok": slim_sf("kospi"),
        "sector_flow_kosdaq_eok": slim_sf("kosdaq"),
        "top_stocks_eok": slim_ts,
        "theme_etf": None if (p.get("themes") or {}).get("stale") else {
            per: [{"theme": t["theme"], "median": t["median"], "ace_median": t.get("ace_median"),
                   "ace": [(x["name"], x.get("d1" if per == "daily" else "w1")) for x in t.get("ace", [])[:2]]}
                  for t in (p.get("themes") or {}).get(per, [])]
            for per in ("daily", "weekly")},
        "ace_etf": {per: {kk: ((p.get("ace") or {}).get(per) or {}).get(kk) for kk in ("up", "down", "median", "top", "bottom", "value_top")}
                    for per in ("daily", "weekly")} if not (p.get("ace") or {}).get("stale") else None,
    }


# '~함/~됨/~보임/~해야 함' 등 서술형 어미 → 명사형 종결로 보정 (모델이 문체 지시를 어길 때 대비)
ENDING_RULES = [
    (r"(을|를)\s*보임$", ""),               # 순매도 우위를 보임 → 순매도 우위
    (r"(을|를)\s+(\S.*?)\s*(으로|로)\s*삼아야\s*함$", r", \2"),  # A를 다음 점검 포인트로 삼아야 함 → A, 다음 점검 포인트
    (r"(으로|로)\s*삼아야\s*함$", ""),
    (r"(해야|되어야|돼야)\s*함$", " 필요"),   # 확인해야 함 → 확인 필요
    (r"할\s*필요가\s*있음$", " 필요"),
    (r"필요함$", "필요"),
    (r"(이|가)\s*있음$", ""),                # 부담이 있음 → 부담
    (r"\s*(하였|했|되었|됐)음$", ""),          # 반등했음 → 반등
    (r"([가-힣A-Za-z0-9%)])\s*(함|됨)$", r"\1"),  # 탈환함 → 탈환, 확대됨 → 확대
    (r"\s*(했|하였|됐|되었)다$", ""),
    (r"([가-힣])(한다|된다|이다)$", r"\1"),
]


def noun_ending(text: str) -> str:
    t = str(text).strip()
    cite = re.search(r"(\s*\[[\d,\s]+\])+$", t)  # 끝의 [1] 인용은 떼었다가 다시 붙임
    tail = cite.group(0) if cite else ""
    t = t[: len(t) - len(tail)].rstrip(" .。")
    for rx, sub in ENDING_RULES:
        new = re.sub(rx, sub, t)
        if new != t:
            t = new.rstrip(" ,")
            break
    return t + tail


def polish_brief(blk: dict) -> dict:
    blk["headline"] = noun_ending(blk.get("headline", ""))
    blk["summary"] = [noun_ending(x) for x in blk.get("summary", [])]
    blk["strategy"] = [noun_ending(x) for x in blk.get("strategy", [])]
    for sc in blk.get("sections", []):
        sc["gist"] = noun_ending(sc.get("gist", ""))
        for it in sc.get("items", []):
            it["v"] = noun_ending(it.get("v", ""))
    return blk


def to_kv(text: str) -> dict:
    t = str(text).strip()
    m = re.match(r"^([^:：]{1,16})[:：]\s*(.+)$", t)
    return {"k": m.group(1).strip(), "v": m.group(2).strip()} if m else {"k": "", "v": t}


def normalize_section(raw) -> dict:
    """Gemini 응답의 파트를 {gist, items:[{k,v}]}로 통일 (예전 문자열 목록 형식도 허용)."""
    gist, items = "", []
    if isinstance(raw, dict):
        gist = str(raw.get("gist", "")).strip()
        raw_items = raw.get("items") or []
    elif isinstance(raw, list):
        raw_items = raw
    else:
        raw_items = []
    for it in raw_items[:5]:
        if isinstance(it, dict):
            k, v = str(it.get("k", "")).strip(), str(it.get("v", "")).strip()
            if v:
                items.append({"k": k, "v": v})
        elif str(it).strip():
            items.append(to_kv(it))
    return {"gist": gist, "items": items}


def clean_list(v, n):
    if not isinstance(v, list):
        return []
    return [str(x).strip() for x in v if str(x).strip()][:n]


def generate_briefing(p: dict) -> dict:
    fallback = build_rule_briefing(p)
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        log("GEMINI_API_KEY 없음 → 규칙 기반 브리핑 사용")
        return fallback

    news = [f"[{n['id']}] {n['title']} ({n['source']}, {n['published']})" for n in (p.get("news") or {}).get("items", [])]
    prompt = (
        "당신은 한국 증시 시니어 리서치 애널리스트입니다. 아래 장 마감 데이터와 기사 헤드라인으로 '시황 브리핑'을 작성하세요.\n"
        "작성 원칙 (읽는 사람이 30초 안에 오늘 시장을 이해하도록):\n"
        "1) headline: 오늘 시장의 결론 한 문장(40자 내외). '무엇 때문에 어떻게 됐다'가 드러나게.\n"
        "2) summary(총평): 핵심 메시지 3개. 각 1문장, 반드시 명사로 끝맺음(예: '기관 매수에 코스피 1.99% 반등, 5,000선 탈환' O / '…탈환함' X). ①시장 방향과 원인 ②돈의 흐름(누가 사고 팔았나) ③다음에 볼 것. 수치는 꼭 필요한 것만.\n"
        "3) sections: 파트별 근거. 각 파트는 {\"gist\": 요지, \"items\": [{\"k\": 항목명, \"v\": 내용}]} 형태.\n"
        "   - gist: 그 파트의 결론 한 줄(25자 내외, 숫자 없이 의미 위주). 예) '외국인 매도 vs 기관·개인 매수', '방어 업종만 선방', '반도체 대형주에 매도 집중'\n"
        "   - items 2~4개. k는 2~8자 명사(예: 코스피, 외국인, 전기전자, 삼성전자, 시장 폭), v는 수치 포함 핵심 내용 35자 내외.\n"
        "     v에 k를 반복하지 말 것. 예) {\"k\":\"외국인\",\"v\":\"코스피 -1조 4,507억 원 순매도\"}\n"
        "   - 앞 파트와 중복 금지.\n"
        "   - market(지수·매크로): 지수·환율·시장 폭. 국내 장에서 실제로 벌어진 일 중심.\n"
        "   - flow(수급): 외국인·기관·개인 순매수, 업종별 수급 쏠림\n"
        "   - sector(업종): 강세·약세 업종과 그 의미(순환매/방어 등)\n"
        "   - stock(종목): 투자자별 주요 순매수·순매도 종목\n"
        "   - issue(이슈·뉴스): 기사 헤드라인에서 확인되는 원인·이벤트, 항목 끝에 [번호] 인용\n"
        "   - ace(ACE ETF): 한국투자신탁운용 ACE ETF 라인업 마감 흐름. ace_etf·theme_etf 데이터만 사용.\n"
        "     어떤 테마의 ACE 상품이 선방/부진했는지(theme_etf의 테마 중간값과 비교), 상승·하락 상위, 거래대금 상위를 2~4개 항목으로.\n"
        "     반드시 'ACE '로 시작하는 상품만 언급. KODEX·TIGER·SOL·RISE 등 타사 상품은 절대 언급 금지. ace_etf가 null이면 ace 파트는 빈 items로.\n"
        "     상품명은 데이터 그대로, 투자 권유 표현(추천·매수 기회·입증 등) 금지.\n"
        "4) strategy(대응 포인트): 2~3개, 다음 거래일 점검 포인트와 대응.\n"
        "- 문체(전 항목 공통, 증권사 시황 리포트 스타일): 명사로 끝맺음. 예) '코스피 -3.59% 급락', '외국인 1조 3,300억 원 순매도', '방어주 강세', '반도체 수급 이탈 지속'.\n"
        "  '~함', '~했음', '~기록함', '~보임', '~됨', '~다' 로 끝내지 말 것. 군더더기 없이 짧게(한 항목 60자 내외).\n"
        "- 이 브리핑은 아침 'ACE 모닝콜'에 이어지는 장 마감 피드백임. indices.sox(필라델피아 반도체)와 전일 밤 미국장 소식은 모닝콜에서 이미 다룸.\n"
        "  → '전일 미 반도체 급락 영향 반영'처럼 원인 배경으로 한 번만 짧게 언급하고, SOX 수치를 독립 항목으로 반복하지 말 것.\n"
        "  → 대신 국내 장에서 그 영향이 어떻게 나타났는지(어느 업종·종목·수급으로 번졌는지, 예상보다 강했는지 약했는지)에 집중.\n"
        "- spotlight_overnight는 해외·변동성 지표의 전일(또는 당일) 변화와 평소 대비 배수(z). z≥2인 지표만 '배경'으로 짧게 언급 가능, 나머지는 언급 금지.\n"
        "- 지수 레벨 표현은 실제 수치와 일치시킬 것(예: 5,100.50pt면 '5100선 붕괴'가 아니라 '5100선 턱걸이'). 기사 제목의 표현보다 데이터를 우선.\n"
        "- 수치 우선순위: 지수·수급·업종·종목 수치와 방향(순매수/순매도)은 반드시 [데이터]의 값만 사용. 기사 제목의 수치(예: '1.5조')와 다르면 데이터를 따를 것.\n"
        f"  오늘 코스피 수급(억 원): 외국인 {p['flow']['kospi'].get('daily_foreign')}, 기관 {p['flow']['kospi'].get('daily_inst')}, 개인 {p['flow']['kospi'].get('daily_retail')} (음수=순매도, 양수=순매수)\n"
        "- 기사에 없는 사실·수치는 지어내지 말 것. 다른 파트에서도 기사 근거가 있으면 [번호] 인용 가능.\n"
        "- daily는 일간, weekly는 최근 5거래일 누적 기준. stale=true 항목은 언급 금지.\n"
        "- 숫자 표기: 수급 단위는 억 원. 1만억 이상은 조 단위로 환산(예: -13300 → -1조 3,300억 원), 그 미만은 천 단위 콤마(예: -4,635억 원). 지수는 5,100.50pt 형식.\n"
        "5) ace_picks(오늘의 ACE ETF): [ACE 후보]에서 오늘 시장 흐름과 연결해 살펴볼 만한 ACE ETF 6개를 고르고 각각 point(35자 내외)를 작성.\n"
        "   - 반드시 후보의 code만 사용. 가능한 한 서로 다른 테마로 구성(강세 테마 2~3개 + 시장 상황에 맞는 방어/분산 상품 등).\n"
        "   - point는 오늘 데이터와의 연결(예: '반도체 기준지수 -2.2% 속 상대적 선방', '변동성 확대 구간 파킹 수요')을 명사형으로. 추천·매수·유망·수익 보장 표현 금지.\n"
        "반드시 아래 JSON 형식으로만 응답:\n"
        '{"ace_picks":[{"code":"...","point":"..."}],"daily":{"headline":"...","summary":["...","...","..."],'
        '"sections":{"market":{"gist":"...","items":[{"k":"...","v":"..."}]},"flow":{...},"sector":{...},"stock":{...},"issue":{...},"ace":{...}},'
        '"strategy":["..."]},"weekly":{...같은 구조...}}\n\n'
        f"[데이터]\n{json.dumps(briefing_input(p), ensure_ascii=False)}\n\n"
        "[기사 헤드라인]\n" + ("\n".join(news) if news else "(없음)") + "\n\n"
        "[ACE 후보] (code, 상품명, 테마, 1D, 1W, 1M, AUM억)\n" +
        "\n".join(f"{x['code']}, {x['name']}, {x['theme']}, {x.get('d1')}, {x.get('w1')}, {x.get('m1')}, {x.get('aum')}" for x in picks_pool(p))
    )
    S_, A_, O_ = {"type": "STRING"}, (lambda it: {"type": "ARRAY", "items": it}), (lambda props, req=None: {"type": "OBJECT", "properties": props, "required": req or list(props)})
    sec_schema = O_({"gist": S_, "items": A_(O_({"k": S_, "v": S_}))})
    brief_schema = O_({"headline": S_, "summary": A_(S_),
                       "sections": O_({k: sec_schema for k, _ in BRIEF_SECTIONS}, []),
                       "strategy": A_(S_)})
    schema = O_({"ace_picks": A_(O_({"code": S_, "point": S_})), "daily": brief_schema, "weekly": brief_schema})
    # 응답 구조(스키마)를 지정해 JSON이 깨지지 않게 함. 스키마를 못 받는 모델이면 스키마 없이 재시도
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"responseMimeType": "application/json", "responseSchema": schema,
                             "temperature": 0.3, "maxOutputTokens": 16384},
    }
    body_plain = {"contents": body["contents"],
                  "generationConfig": {"responseMimeType": "application/json", "temperature": 0.3, "maxOutputTokens": 16384}}
    headers = {"x-goog-api-key": api_key, "Content-Type": "application/json"}
    for model in GEMINI_MODELS:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        use_body = body
        for attempt in range(1, 3):
            try:
                res = requests.post(url, headers=headers, json=use_body, timeout=90)
                if res.status_code == 400 and use_body is body:
                    log(f"Gemini {model}: 스키마 미지원 추정(400) → 스키마 없이 재시도")
                    use_body = body_plain
                    continue
                if res.status_code != 200:
                    try:
                        err = res.json().get("error", {})
                        detail = f"{err.get('status', '')} {err.get('message', '')}"[:300]
                    except Exception:
                        detail = res.text[:200]
                    log(f"Gemini {model} 실패 (시도 {attempt}/2): HTTP {res.status_code} {detail}")
                    if res.status_code in (400, 401, 403, 404, 429):  # 429(모델별 한도 소진)는 재시도해도 같음 → 다음 모델로
                        break
                    time.sleep(3)
                    continue
                cand = res.json()["candidates"][0]
                if cand.get("finishReason") not in (None, "STOP"):
                    log(f"Gemini {model} 응답 종료 사유: {cand.get('finishReason')}")
                text = cand["content"]["parts"][0]["text"]
                parsed = json.loads(text.replace("```json", "").replace("```", "").strip())
                out = {}
                for per in ("daily", "weekly"):
                    blk = parsed.get(per) or {}
                    headline = str(blk.get("headline", "")).strip()
                    summary = clean_list(blk.get("summary"), 4)
                    raw = blk.get("sections") or {}
                    sections = []
                    ace_ok = not (p.get("ace") or {}).get("stale")
                    for key, title in BRIEF_SECTIONS:
                        sec = normalize_section(raw.get(key) if isinstance(raw, dict) else None)
                        if key == "ace":
                            # 타사 브랜드가 들어간 항목 제거, ACE 데이터 없으면 파트 생략
                            sec["items"] = [it for it in sec["items"] if not OTHER_BRANDS.search(it["k"] + " " + it["v"])] if ace_ok else []
                            if OTHER_BRANDS.search(sec["gist"]):
                                sec["gist"] = ""
                        if sec["items"]:
                            sections.append({"key": key, "title": title, **sec})
                    strategy = clean_list(blk.get("strategy"), 4)
                    if not headline or len(summary) < 2 or len(sections) < 3:
                        raise ValueError(f"{per} 형식 불충분")
                    out[per] = polish_brief({"headline": headline, "summary": summary, "sections": sections,
                                             "strategy": strategy or fallback[per]["strategy"]})
                pool = {x["code"]: x for x in picks_pool(p)}
                gp = []
                for it in (parsed.get("ace_picks") or []):
                    if isinstance(it, dict) and str(it.get("code")) in pool and str(it.get("code")) not in {y["code"] for y in gp}:
                        point = noun_ending(it.get("point", ""))
                        if not OTHER_BRANDS.search(point):
                            gp.append(dict(pool[str(it["code"])], point=point))
                out["ace_picks"] = gp[:6] if len(gp) >= 4 else fallback.get("ace_picks", [])
                out["source"] = "gemini"
                out["model"] = model
                log(f"Gemini 브리핑 생성 완료 ({model})")
                return out
            except Exception as e:
                log(f"Gemini {model} 오류 (시도 {attempt}/2): {type(e).__name__}: {str(e)[:120]}")
            time.sleep(2)
    return fallback


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------
def main() -> int:
    prev = load_previous()
    start_date = (now_kst() - timedelta(days=45)).strftime("%Y-%m-%d")
    krx_login_check()

    indices, kospi_df = fetch_indices(start_date, prev)
    kospi = indices["kospi"]
    if kospi.get("stale") or not kospi.get("price") or kospi_df is None:
        log("KOSPI 수집 실패 → data.json을 덮어쓰지 않고 종료합니다.")
        return 0

    trade_date = kospi["date"]
    prev_trade_date = prev.get("trade_date")
    prev_stale = (prev.get("data_quality") or {}).get("stale_items") or []
    if prev_trade_date and trade_date < prev_trade_date and not FORCE_UPDATE:
        log(f"최신 거래일({trade_date})이 기존 데이터({prev_trade_date})보다 이전 → 갱신 생략.")
        return 0
    prev_post_close = (prev.get("data_quality") or {}).get("news_post_close", NEWS_POST_CLOSE_MIN)
    prev_model = (prev.get("ai_briefing") or {}).get("model")
    news_refresh = (
        prev_trade_date == trade_date
        and (prev_post_close < NEWS_POST_CLOSE_MIN or prev_model != GEMINI_MODELS[0])
        and now_kst().strftime("%Y-%m-%d") == trade_date
        and now_kst().hour < NEWS_REFRESH_UNTIL
    )
    now = now_kst()
    today = now.strftime("%Y-%m-%d")
    is_final_time = (today > trade_date) or (now.hour, now.minute) >= (FINAL_HOUR, FINAL_MIN)
    # KRX 데이터는 심야(21시~07시)에 비거나 오류가 잦음 → 같은 거래일 재수집은 이 시간대에만
    # 운영 구간: 당일 16:20~18:40 (16:30 1차 → 17:15 기사 보강 → 18:05 확정). 그 외 시간은 같은 거래일 갱신 안 함
    in_window = today == trade_date and (16, 20) <= (now.hour, now.minute) < (18, 40)
    prev_final = (prev.get("data_quality") or {}).get("final")
    if prev_final is None:  # 확정 플래그가 없던 예전 데이터 → 갱신 시각으로 판단
        try:
            upd = datetime.strptime(prev.get("updated_at", "")[:16], "%Y-%m-%d %H:%M")
            prev_final = upd.strftime("%Y-%m-%d") > trade_date or (upd.hour, upd.minute) >= (FINAL_HOUR, FINAL_MIN)
        except Exception:
            prev_final = True
    final_refresh = prev_trade_date == trade_date and not prev_final and is_final_time and in_window
    if prev_trade_date == trade_date and not in_window and not FORCE_UPDATE:
        log(f"같은 거래일({trade_date}) 갱신은 당일 16:20~18:40에만 → 생략 (현재 {now:%m-%d %H:%M})")
        return 0
    # 새 거래일 첫 수집도 KRX 데이터가 비는 심야(21시~07시)와 장중(09:00~15:40)은 피함
    if prev_trade_date != trade_date and not FORCE_UPDATE and (
            now.hour >= 21 or now.hour < 7 or (today == trade_date and (now.hour, now.minute) < (15, 40))):
        log(f"새 거래일({trade_date}) 수집 불가 시간대 ({now:%H:%M}) → 생략")
        return 0
    if final_refresh:
        log("18시 이후 → 시간외 거래 반영된 확정 수급으로 갱신")
    if prev_trade_date == trade_date and not prev_stale and not news_refresh and not final_refresh and not FORCE_UPDATE:
        log(f"최신 거래일({trade_date}) 데이터가 이미 완전함 → 휴장일/중복 실행으로 판단, 갱신 생략.")
        return 0
    if news_refresh:
        log(f"장 마감 기사 {prev_post_close}건뿐 → 기사·브리핑 보강 실행")

    dates = [d.strftime("%Y-%m-%d") for d in kospi_df.index]

    def guarded(label, fn, fallback):
        """블록 하나가 예외로 죽어도 전체 수집은 계속 진행."""
        try:
            return fn()
        except Exception as e:
            log(f"{label} 블록 오류 → 이전값 유지: {type(e).__name__}: {e}")
            return fallback()

    sectors = []  # 섹터 대표 ETF는 테마별 ETF(themes)로 대체

    # 실행 단계별 수집 범위 (KRX 호출 최소화)
    #  - 1차(거래일 첫 실행) 또는 이전값 보충: 전부 수집
    #  - 17:15 보강(기사·브리핑 + 지수 종가 재확인): 지수 2회 외 KRX 재호출 없음
    #  - 18:05 확정: 시간외 거래로 바뀌는 수급(시장·업종·종목)만 재수집, ETF·테마·지수는 재사용
    same_day = prev_trade_date == trade_date and not prev_stale and not FORCE_UPDATE
    light = same_day and news_refresh and not final_refresh
    flows_only = same_day and final_refresh
    if light:
        log("실행 범위: 기사·브리핑 보강만 (KRX 데이터 재사용)")
    elif flows_only:
        log("실행 범위: 수급·지수 확정치만 재수집 (ETF·테마 재사용)")
    guarded("지수 기사 교차검증", lambda: verify_indices_with_news(indices, trade_date), lambda: None)

    if light:
        flow = prev.get("flow")
    else:
        flow = guarded("투자자 수급", lambda: fetch_flow(prev, trade_date),
                       lambda: {k: keep_previous(None, (prev.get("flow") or {}).get(k), EMPTY_FLOW) for k, _ in MARKETS})

    def sector_fallback():
        si = {k: keep_previous(None, (prev.get("sector_indices") or {}).get(k), {"daily": [], "weekly": []}) for k, _ in MARKETS}
        br = {k: keep_previous(None, (prev.get("breadth") or {}).get(k), {"up": 0, "down": 0, "flat": 0}) for k, _ in MARKETS}
        sf = dict(prev.get("sector_flow") or {"daily": {}, "weekly": {}}, stale=True)
        ts = dict(prev.get("top_stocks") or {}, stale=True)
        return si, sf, ts, br

    if light:
        sector_indices, sector_flow, top_stocks, breadth = prev.get("sector_indices"), prev.get("sector_flow"), prev.get("top_stocks"), prev.get("breadth")
    else:
        sector_indices, sector_flow, top_stocks, breadth = guarded("업종", lambda: fetch_sector_block(dates, prev), sector_fallback)
    if light or flows_only:
        ace, themes = prev.get("ace") or {"count": 0}, prev.get("themes") or {"daily": [], "weekly": []}
        # 지수는 매 실행마다 새로 읽은 값 사용 (16:30대 KRX 지수가 최종 종가와 다른 경우가 있어 보강·확정 때 바로잡음)
    else:
        first_run_today = prev_trade_date != trade_date  # 구성종목(PDF) 조회는 거래일당 첫 실행에서만
        ace, themes = guarded("ETF", lambda: fetch_etf_block(dates, indices, start_date, allow_pdf=first_run_today), lambda: (None, None))
        ace = keep_previous(ace, prev.get("ace"), {"count": 0})
        themes = keep_previous(themes, prev.get("themes"), {"daily": [], "weekly": []})
    if light or flows_only:
        spotlight = prev.get("spotlight")
    else:
        spotlight = guarded("오늘의 지표", lambda: fetch_spotlight(trade_date, start_date, prev), lambda: prev.get("spotlight"))
    news = guarded("뉴스", lambda: fetch_news(prev, trade_date),
                   lambda: {"items": (prev.get("news") or {}).get("items", []) if isinstance(prev.get("news"), dict) else [], "stale": True})

    payload = {
        "updated_at": now_kst().strftime("%Y-%m-%d %H:%M KST"),
        "trade_date": trade_date,
        "indices": indices,
        "flow": flow,
        "breadth": breadth,
        "sector_indices": sector_indices,
        "sector_flow": sector_flow,
        "top_stocks": top_stocks,
        "sectors": sectors,
        "ace": ace,
        "themes": themes,
        "news": news,
        "spotlight": spotlight,
    }

    stale = [k for k, v in indices.items() if v.get("stale")]
    stale += [f"flow.{k}" for k, v in flow.items() if v.get("stale")]
    stale += [f"breadth.{k}" for k, v in breadth.items() if v.get("stale")]
    stale += [f"sector_indices.{k}" for k, v in sector_indices.items() if v.get("stale")]
    if sector_flow.get("stale") or sector_flow.get("partial"):
        stale.append("sector_flow")
    stale += [s["name"] for s in sectors if s.get("stale")]
    if ace.get("stale"):
        stale.append("ace")
    if themes.get("stale"):
        stale.append("themes")
    # 뉴스는 외부 요인이 커서 재수집 트리거에서 제외
    close_dt = datetime.strptime(trade_date, "%Y-%m-%d").replace(hour=15, minute=30, tzinfo=KST)
    post_close = 0
    for n in news.get("items", []):
        try:
            mmdd, hm = n.get("published", "").split(" ")
            dt = datetime.strptime(f"{close_dt.year}-{mmdd} {hm}", "%Y-%m-%d %H:%M").replace(tzinfo=KST)
            post_close += dt >= close_dt
        except Exception:
            pass
    payload["data_quality"] = {
        "stale_items": stale,
        "is_today": trade_date == now_kst().strftime("%Y-%m-%d"),
        "news_post_close": int(post_close),
        "final": bool(is_final_time),
    }
    hm = now.strftime("%H:%M")
    sox = indices.get("sox", {})
    fx = indices.get("usdkrw", {})
    flow_basis = "시간외 거래 반영 확정치" if is_final_time else "잠정치 (시간외 거래 18:00까지 변동, 18:05 확정)"
    payload["as_of"] = {
        "collected": f"{today} {hm} KST",
        "index": f"한국거래소 {trade_date} 종가" + (" · 마감 기사 교차검증" if any((v or {}).get("verified") == "news" for v in indices.values()) else ""),
        "flow": f"한국거래소 투자자별 매매 · {hm} 수집 · {flow_basis}",
        "breadth": f"한국거래소 {trade_date} 종가 기준 종목 등락",
        "sector": f"한국거래소 업종지수 {trade_date} 종가 · 업종 수급 {hm} 수집",
        "stock": f"한국거래소 투자자별 순매수 · {hm} 수집 · {flow_basis}",
        "etf": f"한국거래소 ETF {trade_date} 종가 기준 등락률",
        "fx": f"원/달러 {fx.get('date') or '-'} 기준 (FinanceDataReader)",
        "sox": f"필라델피아 반도체 {sox.get('date') or '-'} 미국장 종가",
        "spotlight": "오늘의 지표: 해외 지표는 국내 기준일 전날 종가, 닛케이는 당일 · 평소(직전 20일 변동폭) 대비 가장 이례적인 지표 선정",
        "news": f"구글 뉴스 {trade_date} 08:00 ~ {hm}",
    }
    prev_asof = prev.get("as_of") or {}
    if light:  # 재사용한 블록은 원래 수집 시각 유지
        for k in ("flow", "breadth", "sector", "stock", "etf", "fx", "sox", "spotlight"):
            if prev_asof.get(k):
                payload["as_of"][k] = prev_asof[k]
    elif flows_only:
        for k in ("etf",):
            if prev_asof.get(k):
                payload["as_of"][k] = prev_asof[k]

    if prev_trade_date == trade_date and len(stale) > len(prev_stale):
        log(f"이번 수집 결과가 기존보다 나쁨(이전값 {len(prev_stale)}→{len(stale)}개) → 기존 data.json 유지 (강제 실행이어도 동일)")
        return 0

    payload["ai_briefing"] = guarded("브리핑", lambda: generate_briefing(payload), lambda: build_rule_briefing(payload))
    pb = prev.get("ai_briefing") or {}
    if payload["ai_briefing"].get("source") != "gemini" and prev_trade_date == trade_date and pb.get("source") == "gemini":
        # 같은 거래일에 이미 Gemini 브리핑이 있으면 규칙 기반으로 덮지 않고 유지 (다음 실행에서 재시도)
        log("Gemini 실패 → 같은 거래일의 기존 Gemini 브리핑 유지")
        payload["ai_briefing"] = dict(pb, reused=True)

    payload["as_of"]["briefing"] = f"{payload['ai_briefing'].get('model') or '규칙 기반'} · {hm} 작성"
    atomic_write_json(DATA_PATH, payload)
    try:
        archive(payload)
    except Exception as e:
        log(f"날짜별 보관 실패(무시): {type(e).__name__}: {e}")
    log(f"data.json 갱신 완료 (거래일 {trade_date}, 이전값 유지 항목 {len(stale)}개: {stale})")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log(f"치명적 오류: {type(e).__name__}: {e}")
        sys.exit(1)
