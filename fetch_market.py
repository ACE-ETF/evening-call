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


def krx_call(label: str, fn, *args, retries: int = 2):
    """pykrx 호출 공통 래퍼: 빈 결과/오류 시 재시도 후 None."""
    if stock is None:
        return None
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
        if key == "kospi" and metrics is not None:
            cutoff = metrics["date"]
            kospi_df = cut(df, cutoff)
        out[key] = keep_previous(metrics, prev_indices.get(key), default)
    return out, kospi_df


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
        sector_names = set(norm(x) for x in cls_df["업종명"].unique()) if cls_df is not None and "업종명" in cls_df.columns else set()
        sector_indices[key] = get_sector_indices(market, dates, sector_names)

        for period, (s, e) in periods.items():
            agg = {}
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
    ("채권혼합", r"채권혼합|채혼합|국채혼합|TDF|TRF|자산배분"),
    ("채권·금리·파킹", r"국채|채권|회사채|크레딧|금리|CD|KOFR|SOFR|머니마켓|단기|통안|하이일드|국공채|미국채|T-?Bill|채\b"),
    ("리츠·부동산", r"리츠|부동산|REIT"),
    ("반도체", r"반도체|하이닉스|삼성전자|SK하이닉스|HBM|ASML|TSMC"),
    ("전력·원전·인프라", r"전력|원자력|원전|SMR|전선|그리드|인프라|데이터센터|송전"),
    ("AI·소프트웨어", r"AI|인공지능|소프트웨어|클라우드|로봇|휴머노이드|양자|플랫폼|인터넷"),
    ("2차전지", r"2차전지|이차전지|배터리|리튬|전기차|EV|양극재"),
    ("바이오·헬스케어", r"바이오|헬스케어|제약|빅파마|일라이릴리|비만|의료|헬스"),
    ("조선·방산·우주", r"조선|방산|방위|우주|항공|K-?방산|기계|중공업"),
    ("금융·밸류업", r"은행|금융|증권|보험|밸류업|지주|PBR"),
    ("배당·인컴", r"배당|커버드콜|인컴|월지급|프리미엄"),
    ("미국 빅테크·성장", r"빅테크|테크|FANG|엔비디아|테슬라|애플|마이크로소프트|구글|알파벳|아마존|메타|브로드컴|팔란티어|M7|매그니피센트"),
    ("미국 대표지수", r"S&P500|S&P 500|나스닥|NASDAQ|다우존스|러셀|미국\s?MSCI|미국대표|미국 대표|미국S&P"),
    ("인도·신흥국", r"인도|베트남|인도네시아|멕시코|브라질|신흥|아세안|중남미|사우디"),
    ("중국·일본·유럽", r"중국|차이나|China|항셍|홍콩|일본|TOPIX|니케이|Nikkei|유로|유럽|독일|글로벌"),
    ("금·원자재", r"금현물|골드|금선물|금액티브|은선물|원유|WTI|구리|원자재|금속|천연가스|농산물"),
    ("K컬처·소비", r"KPOP|K-POP|미디어|엔터|게임|화장품|뷰티|소비|음식료|여행|K컬처|컨텐츠|콘텐츠"),
    ("국내 대표지수", r" 200(TR)?$| 200 |코스피|KOSPI|코스닥|KOSDAQ|KRX300|KRX 300|MSCI Korea|MSCI코리아|TOP10$|대형주|중소형"),
]
THEME_RE = [(n, re.compile(rx, re.I)) for n, rx in THEMES]
LEV_INV = re.compile(r"레버리지|인버스|2X|곱버스|선물인버스|-1X|2배|3배", re.I)
OTHER_BRANDS = re.compile(r"\b(KODEX|TIGER|SOL|RISE|KBSTAR|HANARO|PLUS|KIWOOM|KOSEF|ARIRANG|1Q|TIME|TIMEFOLIO|BNK|WON|마이티|파워|UNICORN|FOCUS|에셋플러스|히어로즈|VITA|DAISHIN343|TRUSTON|마이다스|HK|KCGI|BNK|하나)\b")


def theme_of(name: str):
    if LEV_INV.search(name):
        return None
    for n, rx in THEME_RE:
        if rx.search(name):
            return n
    return None


def etf_rows(df, names: dict):
    df = df.copy()
    df.index = df.index.astype(str)
    for c in ("등락률", "거래대금", "종가"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    df = df.dropna(subset=["등락률"])
    return [{"code": c, "name": str(names.get(c, c)), "pct": round(float(r["등락률"]), 2),
             "price": int(r["종가"] or 0), "value": eok(r["거래대금"] or 0)}
            for c, r in df.iterrows() if c in names]


def build_themes(rows: list) -> list:
    groups = {}
    for r in rows:
        t = theme_of(r["name"])
        if t:
            groups.setdefault(t, []).append(r)
    out = []
    for t, _ in THEMES:
        g = groups.get(t) or []
        if len(g) < 2:
            continue
        ace = [x for x in g if x["name"].startswith(ACE_PREFIX)]
        others = [x for x in g if not x["name"].startswith(ACE_PREFIX)]
        out.append({
            "theme": t,
            "median": round(float(pd.Series([x["pct"] for x in g]).median()), 2),
            "count": len(g),
            "ace_count": len(ace),
            "ace_median": round(float(pd.Series([x["pct"] for x in ace]).median()), 2) if ace else None,
            # ACE: 거래대금 큰 순으로 최대 6개 / 타사 비교: 거래대금 상위 3개
            "ace": sorted(ace, key=lambda x: x["value"], reverse=True)[:6],
            "others": sorted(others, key=lambda x: x["value"], reverse=True)[:3],
        })
    return sorted(out, key=lambda x: x["median"], reverse=True)


def fetch_etf_block(dates: list):
    """ETF 전종목 등락률 1회 조회(기간별) → ACE 라인업 요약 + 테마별 묶음."""
    names = etf_names()
    if not names:
        return None, None
    ace_codes = {c: n for c, n in names.items() if str(n).startswith(ACE_PREFIX)}
    trade = dates[-1]
    ace = {"count": len(ace_codes)}
    themes = {}
    # KRX 기간등락률은 시작일의 전일 종가 기준 → 일간 (당일~당일), 5일 (5거래일 전~당일)
    for period, start in (("daily", trade), ("weekly", dates[-5] if len(dates) >= 5 else dates[0])):
        df = krx_call(f"ETF 등락률({period})", stock.get_etf_price_change_by_ticker if stock else None, ymd(start), ymd(trade))
        if df is None or "등락률" not in df.columns:
            return None, None
        rows = etf_rows(df, names)
        if period == "daily":
            k200 = next((x for x in rows if x["code"] == "069500"), None)
            if k200:
                log(f"ETF 등락률 검증: KODEX 200 {k200['pct']:.2f}%")
        themes[period] = build_themes(rows)
        arows = [x for x in rows if x["code"] in ace_codes]
        if not arows:
            return None, None
        by_pct = sorted(arows, key=lambda x: x["pct"], reverse=True)
        r = pd.Series([x["pct"] for x in arows])
        ace[period] = {
            "up": int((r > 0).sum()), "down": int((r < 0).sum()), "flat": int((r == 0).sum()),
            "median": round(float(r.median()), 2),
            "top": by_pct[:8],
            "bottom": by_pct[-8:][::-1],
            "value_top": sorted(arows, key=lambda x: x["value"], reverse=True)[:8],
        }
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
    return out


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
        "flow_eok": p["flow"],
        "breadth": p.get("breadth"),
        "sector_index_kospi": slim_si("kospi"),
        "sector_index_kosdaq": slim_si("kosdaq"),
        "sector_flow_kospi_eok": slim_sf("kospi"),
        "sector_flow_kosdaq_eok": slim_sf("kosdaq"),
        "top_stocks_eok": slim_ts,
        "theme_etf": None if (p.get("themes") or {}).get("stale") else {
            per: [{"theme": t["theme"], "median": t["median"], "ace_median": t.get("ace_median"),
                   "ace": [(x["name"], x["pct"]) for x in t.get("ace", [])[:2]]}
                  for t in (p.get("themes") or {}).get(per, [])]
            for per in ("daily", "weekly")},
        "ace_etf": {per: {kk: ((p.get("ace") or {}).get(per) or {}).get(kk) for kk in ("up", "down", "median", "top", "bottom", "value_top")}
                    for per in ("daily", "weekly")} if not (p.get("ace") or {}).get("stale") else None,
    }


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
        "2) summary(총평): 핵심 메시지 3개. 각 1문장. ①시장 방향과 원인 ②돈의 흐름(누가 사고 팔았나) ③다음에 볼 것. 수치는 꼭 필요한 것만.\n"
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
        "- 지수 레벨 표현은 실제 수치와 일치시킬 것(예: 5,100.50pt면 '5100선 붕괴'가 아니라 '5100선 턱걸이'). 기사 제목의 표현보다 데이터를 우선.\n"
        "- 수치 우선순위: 지수·수급·업종·종목 수치와 방향(순매수/순매도)은 반드시 [데이터]의 값만 사용. 기사 제목의 수치(예: '1.5조')와 다르면 데이터를 따를 것.\n"
        f"  오늘 코스피 수급(억 원): 외국인 {p['flow']['kospi'].get('daily_foreign')}, 기관 {p['flow']['kospi'].get('daily_inst')}, 개인 {p['flow']['kospi'].get('daily_retail')} (음수=순매도, 양수=순매수)\n"
        "- 기사에 없는 사실·수치는 지어내지 말 것. 다른 파트에서도 기사 근거가 있으면 [번호] 인용 가능.\n"
        "- daily는 일간, weekly는 최근 5거래일 누적 기준. stale=true 항목은 언급 금지.\n"
        "- 숫자 표기: 수급 단위는 억 원. 1만억 이상은 조 단위로 환산(예: -13300 → -1조 3,300억 원), 그 미만은 천 단위 콤마(예: -4,635억 원). 지수는 5,100.50pt 형식.\n"
        "반드시 아래 JSON 형식으로만 응답:\n"
        '{"daily":{"headline":"...","summary":["...","...","..."],'
        '"sections":{"market":{"gist":"...","items":[{"k":"...","v":"..."}]},"flow":{...},"sector":{...},"stock":{...},"issue":{...},"ace":{...}},'
        '"strategy":["..."]},"weekly":{...같은 구조...}}\n\n'
        f"[데이터]\n{json.dumps(briefing_input(p), ensure_ascii=False)}\n\n"
        "[기사 헤드라인]\n" + ("\n".join(news) if news else "(없음)")
    )
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"responseMimeType": "application/json", "temperature": 0.3},
    }
    headers = {"x-goog-api-key": api_key, "Content-Type": "application/json"}
    for model in GEMINI_MODELS:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        for attempt in range(1, 3):
            try:
                res = requests.post(url, headers=headers, json=body, timeout=60)
                if res.status_code != 200:
                    try:
                        err = res.json().get("error", {})
                        detail = f"{err.get('status', '')} {err.get('message', '')}"[:300]
                    except Exception:
                        detail = res.text[:200]
                    log(f"Gemini {model} 실패 (시도 {attempt}/2): HTTP {res.status_code} {detail}")
                    if res.status_code in (400, 401, 403, 404):
                        break
                    time.sleep(3)
                    continue
                text = res.json()["candidates"][0]["content"]["parts"][0]["text"]
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
                    out[per] = {"headline": headline, "summary": summary, "sections": sections,
                                "strategy": strategy or fallback[per]["strategy"]}
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
    in_window = (today == trade_date and (15, 40) <= (now.hour, now.minute) < (21, 0)) or \
                (today > trade_date and 7 <= now.hour < 9)
    prev_final = (prev.get("data_quality") or {}).get("final")
    if prev_final is None:  # 확정 플래그가 없던 예전 데이터 → 갱신 시각으로 판단
        try:
            upd = datetime.strptime(prev.get("updated_at", "")[:16], "%Y-%m-%d %H:%M")
            prev_final = upd.strftime("%Y-%m-%d") > trade_date or (upd.hour, upd.minute) >= (FINAL_HOUR, FINAL_MIN)
        except Exception:
            prev_final = True
    final_refresh = prev_trade_date == trade_date and not prev_final and is_final_time and in_window
    if prev_trade_date == trade_date and not in_window and not FORCE_UPDATE:
        log(f"같은 거래일({trade_date}) 재수집은 15:40~21:00 / 다음날 07~09시에만 → 생략 (현재 {now:%H:%M})")
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
    flow = guarded("투자자 수급", lambda: fetch_flow(prev, trade_date),
                   lambda: {k: keep_previous(None, (prev.get("flow") or {}).get(k), EMPTY_FLOW) for k, _ in MARKETS})

    def sector_fallback():
        si = {k: keep_previous(None, (prev.get("sector_indices") or {}).get(k), {"daily": [], "weekly": []}) for k, _ in MARKETS}
        br = {k: keep_previous(None, (prev.get("breadth") or {}).get(k), {"up": 0, "down": 0, "flat": 0}) for k, _ in MARKETS}
        sf = dict(prev.get("sector_flow") or {"daily": {}, "weekly": {}}, stale=True)
        ts = dict(prev.get("top_stocks") or {}, stale=True)
        return si, sf, ts, br

    sector_indices, sector_flow, top_stocks, breadth = guarded("업종", lambda: fetch_sector_block(dates, prev), sector_fallback)
    ace, themes = guarded("ETF", lambda: fetch_etf_block(dates), lambda: (None, None))
    ace = keep_previous(ace, prev.get("ace"), {"count": 0})
    themes = keep_previous(themes, prev.get("themes"), {"daily": [], "weekly": []})
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

    if prev_trade_date == trade_date and len(stale) > len(prev_stale) and not FORCE_UPDATE:
        log(f"이번 수집 결과가 기존보다 나쁨(이전값 {len(prev_stale)}→{len(stale)}개) → 기존 data.json 유지")
        return 0

    payload["ai_briefing"] = guarded("브리핑", lambda: generate_briefing(payload), lambda: build_rule_briefing(payload))

    atomic_write_json(DATA_PATH, payload)
    log(f"data.json 갱신 완료 (거래일 {trade_date}, 이전값 유지 항목 {len(stale)}개: {stale})")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log(f"치명적 오류: {type(e).__name__}: {e}")
        sys.exit(1)
