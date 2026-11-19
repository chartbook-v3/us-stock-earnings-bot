"""
=============================================================================
US Stock Earnings Intelligence Bot v2 (미국 기업 실시간 실적 트래킹 봇)
=============================================================================
- 감시 대상: US Stock Bot v12 전체 유니버스 (S&P 500 + NASDAQ 100 + Russell 3000)
- 수집 방식 (방식 B):
  1) 장전(ET 05:30) Nasdaq 실적 일정 + Yahoo Finance 컨센서스(0q, +1q, 0y, +1y) 사전 스냅샷 박제
  2) 매일 장전(ET 06:00+) 주요 실적발표 일정(@uswatch3) 1회 사전 브리핑
  3) 실시간 SEC EDGAR 8-K (Item 2.02) & 6-K Atom/EFTS 감시 -> Exhibit 99.1/99.2 본문 추출
  4) Gemini API (`gemini-4-hydrogen`) Structured Output으로 실적(매출·EPS·GPM·OPM) 및
     다음분기/다음 회계연도 가이던스(매출·EPS·CAPEX·GPM·OPM) 정밀 추출 + Google Search 기반 보조 컨센 조회
  5) 월가 컨센서스 보유 종목만 엄선하여 심플 포맷으로 텔레그램 즉시 브리핑
=============================================================================
"""

import os
import sys
import io
import re
import csv
import json
import time
import html
import math
import argparse
import threading
import datetime
import importlib.util
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

# [Windows 콘솔 한글/UTF-8 입출력 안정화 및 headless 로그 파일 폴백]
if sys.platform.startswith("win"):
    try:
        if sys.stdout is None or sys.stderr is None:
            _log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "v2_earnings_schedule.log")
            _log_file = open(_log_path, "a", encoding="utf-8", buffering=1, errors="replace")
            if sys.stdout is None:
                sys.stdout = _log_file
            if sys.stderr is None:
                sys.stderr = _log_file
        else:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
            sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass

_SINGLETON_LOCK_FD = None


def acquire_single_instance_lock(lock_name: str) -> bool:
    """동일 봇 프로세스 중복 실행 방지용 Windows 논블로킹 파일 락을 획득합니다."""
    if sys.platform != "win32":
        return True
    import msvcrt
    lock_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), lock_name)
    fd = None
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        global _SINGLETON_LOCK_FD
        _SINGLETON_LOCK_FD = fd
        return True
    except OSError:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        return False


import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from curl_cffi import requests as cf_requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

try:
    from google import genai
    from google.genai import types as genai_types
    HAS_GENAI = True
except ImportError:
    genai = None
    genai_types = None
    HAS_GENAI = False

# =====================================================================
# 1. 전역 경로, 타임존 및 설정 로더
# =====================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.environ.get("STATE_DIR") or BASE_DIR
CACHE_DIR = os.environ.get("CACHE_DIR") or BASE_DIR

CONFIG_PATH = os.path.join(BASE_DIR, "us_earnings_config.json")
UNIVERSE_CACHE_PATH = os.path.join(CACHE_DIR, "us_universe_cache.json")
SEC_CIK_CACHE_PATH = os.path.join(CACHE_DIR, "sec_cik_map_cache.json")
KOREAN_NAMES_CACHE_PATH = os.path.join(STATE_DIR, "us_korean_names_cache.json")
SNAPSHOT_PATH = os.path.join(STATE_DIR, "daily_consensus_snapshot.json")
SEEN_CACHE_PATH = os.path.join(STATE_DIR, "seen_us_earnings.json")

ET = ZoneInfo("America/New_York")
KST = datetime.timezone(datetime.timedelta(hours=9))

_thread_local = threading.local()
_cache_lock = threading.Lock()

MAX_FILINGS_PER_RUN = 8

DEFAULT_CONFIG = {
    "gemini_api_key": "",
    "gemini_model": "gemini-4-hydrogen",
    "gemini_search_model": "gemini-4-hydrogen",
    "enable_grounded_margin_capex_consensus": False,
    "telegram_bot_token": "",
    "telegram_chat_id": "@uswatch3",
    "sec_user_agent": "USStockEarningsBot/2.0 (contact: chartbook@example.com)",
    "major_schedule_min_mcap_usd": 10_000_000_000,
    "major_schedule_max_per_session": 15,
    "active_poll_interval_sec": 45,
    "regular_poll_interval_sec": 240,
    "idle_poll_interval_sec": 300,
}


def load_config() -> Dict[str, Any]:
    """us_earnings_config.json 및 환경변수에서 설정을 로드합니다."""
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                cfg.update(loaded)
        except Exception as e:
            print(f"[경고] 설정 파일 로드 오류 (기본값 사용): {e}")

    for env_name, cfg_key in (
        ("GEMINI_API_KEY", "gemini_api_key"),
        ("TELEGRAM_BOT_TOKEN", "telegram_bot_token"),
        ("TELEGRAM_CHAT_ID", "telegram_chat_id"),
        ("SEC_USER_AGENT", "sec_user_agent"),
    ):
        env_val = os.environ.get(env_name, "").strip()
        if env_val:
            cfg[cfg_key] = env_val
        else:
            cfg[cfg_key] = str(cfg.get(cfg_key) or "").strip()

    env_grounded = os.environ.get("ENABLE_GROUNDED_CONSENSUS", "").strip().lower()
    if env_grounded in ("true", "1", "yes"):
        cfg["enable_grounded_margin_capex_consensus"] = True
    elif env_grounded in ("false", "0", "no"):
        cfg["enable_grounded_margin_capex_consensus"] = False

    return cfg


def get_thread_cf_session() -> cf_requests.Session:
    """스레드별 curl_cffi Chrome 위장 세션 (사내 프록시 SSL 검증 우회 적용)."""
    if not hasattr(_thread_local, "cf_session"):
        _thread_local.cf_session = cf_requests.Session(impersonate="chrome", verify=False)
    return _thread_local.cf_session


def load_json_file(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json_file(path: str, data: Any) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp_path = path + ".tmp"
    with _cache_lock:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)


# =====================================================================
# 2. Pydantic 구조화 출력 스키마 (Gemini Method B 전용)
# =====================================================================
class MetricGuidance(BaseModel):
    low: Optional[float] = Field(default=None, description="Lower bound of guided range. Monetary in USD millions; margins in percentage (e.g. 74.5); EPS in USD.")
    high: Optional[float] = Field(default=None, description="Upper bound of guided range. Monetary in USD millions; margins in percentage; EPS in USD.")
    point: Optional[float] = Field(default=None, description="Single point estimate if no range is given. Monetary in USD millions; margins in percentage; EPS in USD.")


class QuarterActuals(BaseModel):
    revenue_musd: Optional[float] = Field(default=None, description="Reported quarterly revenue in USD millions.")
    eps_actual: Optional[float] = Field(default=None, description="Reported quarterly diluted EPS in USD (prefer Non-GAAP/Adjusted if reported; otherwise GAAP).")
    eps_basis: Optional[str] = Field(default=None, description="'Non-GAAP' or 'GAAP'")
    gpm_pct: Optional[float] = Field(default=None, description="Quarterly gross profit margin in percent (e.g. 74.8), preferring Non-GAAP if both reported.")
    opm_pct: Optional[float] = Field(default=None, description="Quarterly operating profit margin in percent (e.g. 61.5), preferring Non-GAAP if both reported.")


class PeriodGuidance(BaseModel):
    has_guidance: bool = Field(default=False, description="True if the filing explicitly provides forward guidance for this period.")
    period_label: Optional[str] = Field(default=None, description="Label such as 'Q4 FY27' or 'FY2027'.")
    fiscal_year: Optional[int] = Field(default=None, description="4-digit fiscal year if stated, e.g. 2027.")
    revenue: Optional[MetricGuidance] = None
    eps: Optional[MetricGuidance] = None
    capex: Optional[MetricGuidance] = None
    gpm: Optional[MetricGuidance] = None
    opm: Optional[MetricGuidance] = None


class EarningsExtraction(BaseModel):
    is_earnings_release: bool = Field(default=True, description="True if document contains quarterly or annual earnings results.")
    fiscal_quarter_label: Optional[str] = Field(default=None, description="Reported period label such as 'Q3 FY27' or 'Q3 2026'.")
    quarter_end_date: Optional[str] = Field(default=None, description="Quarter end date in YYYY-MM-DD if stated.")
    actuals: QuarterActuals
    next_quarter_guidance: Optional[PeriodGuidance] = None
    full_year_guidance: Optional[PeriodGuidance] = None


class SupplementalConsensus(BaseModel):
    next_q_capex_cons_musd: Optional[float] = Field(default=None, description="Wall Street consensus CAPEX for next quarter in USD millions, or null if not verified.")
    next_q_gpm_cons_pct: Optional[float] = Field(default=None, description="Wall Street consensus gross margin % for next quarter, or null if not verified.")
    next_q_opm_cons_pct: Optional[float] = Field(default=None, description="Wall Street consensus operating margin % for next quarter, or null if not verified.")
    fy_capex_cons_musd: Optional[float] = Field(default=None, description="Wall Street consensus CAPEX for guided fiscal year in USD millions, or null if not verified.")
    fy_gpm_cons_pct: Optional[float] = Field(default=None, description="Wall Street consensus gross margin % for guided fiscal year, or null if not verified.")
    fy_opm_cons_pct: Optional[float] = Field(default=None, description="Wall Street consensus operating margin % for guided fiscal year, or null if not verified.")


# =====================================================================
# 3. v12 유니버스 로더, SEC CIK 매핑 & 한글 종목명 리졸버
# =====================================================================
def normalize_ticker(sym: Any) -> str:
    """티커 규격 통일 (예: BRK.B / BRK B -> BRK-B)."""
    if sym is None:
        return ""
    return str(sym).strip().upper().replace(".", "-").replace("/", "-").replace(" ", "-")


def _is_valid_us_equity_ticker(sym: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]{1,5}(-[A-Z])?", sym))


def _fetch_v12_universe_standalone():
    """
    pandas/yfinance 없이 순수 Python(curl_cffi + BeautifulSoup + csv)으로
    S&P 500 + NASDAQ 100 + Russell 3000 통합 유니버스를 구축합니다. (클라우드/GitHub Actions 호환)
    """
    sess = get_thread_cf_session()
    sp500: Dict[str, str] = {}
    ndx100: Dict[str, str] = {}
    r3000: Dict[str, str] = {}

    # 1. S&P 500 (Wikipedia -> Slickcharts 폴백)
    try:
        r = sess.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", timeout=15)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "lxml")
            tbl = soup.find("table", {"id": "constituents"}) or soup.find("table", {"class": "wikitable"})
            if tbl:
                for tr in tbl.find_all("tr")[1:]:
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        sym = normalize_ticker(tds[0].get_text(strip=True))
                        if _is_valid_us_equity_ticker(sym):
                            sp500[sym] = tds[1].get_text(strip=True) or sym
    except Exception:
        pass
    if len(sp500) < 400:
        try:
            r = sess.get("https://www.slickcharts.com/sp500", timeout=15)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "lxml")
                tbl = soup.find("table")
                if tbl:
                    for tr in tbl.find_all("tr")[1:]:
                        tds = tr.find_all("td")
                        if len(tds) >= 3:
                            sym = normalize_ticker(tds[2].get_text(strip=True))
                            if _is_valid_us_equity_ticker(sym):
                                sp500[sym] = tds[1].get_text(strip=True) or sym
        except Exception:
            pass

    # 2. NASDAQ 100
    try:
        r = sess.get(
            "https://api.nasdaq.com/api/quote/list-type/nasdaq100",
            headers={"Accept": "application/json", "Origin": "https://www.nasdaq.com", "Referer": "https://www.nasdaq.com/"},
            timeout=15,
        )
        if r.status_code == 200:
            rows = r.json().get("data", {}).get("data", {}).get("rows", []) or []
            for row in rows:
                sym = normalize_ticker(row.get("symbol", ""))
                if _is_valid_us_equity_ticker(sym):
                    ndx100[sym] = str(row.get("companyName", sym)).strip()
    except Exception:
        pass

    # 3. Russell 3000 (iShares IWV CSV)
    try:
        r = sess.get(
            "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/latest-holdings.csv",
            timeout=25,
        )
        if r.status_code == 200 and "Ticker,Name," in r.text:
            lines = r.text.splitlines()
            header_idx = next((i for i, ln in enumerate(lines) if ln.startswith("Ticker,Name,")), None)
            if header_idx is not None:
                reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
                for row in reader:
                    if str(row.get("Asset Class", "")).strip() != "Equity":
                        continue
                    sym = normalize_ticker(row.get("Ticker", ""))
                    if _is_valid_us_equity_ticker(sym):
                        r3000[sym] = str(row.get("Name", sym)).strip()
    except Exception:
        pass

    merged: Dict[str, str] = {}
    merged.update(r3000)
    merged.update(ndx100)
    merged.update(sp500)
    return merged, {"SP500": len(sp500), "NDX100": len(ndx100), "R3000": len(r3000), "TOTAL": len(merged)}


def load_v12_universe(force_refresh: bool = False):
    """
    S&P 500 + NASDAQ 100 + Russell 3000 통합 유니버스를 로드합니다.
    캐시 미존재 또는 24시간 초과 시 내장 경량 로더(실패 시 v12 로더/만료 캐시 폴백)로 갱신합니다.
    """
    now_ts = time.time()
    cached = load_json_file(UNIVERSE_CACHE_PATH, {})
    cached_tickers = cached.get("tickers", [])
    age_hours = (now_ts - float(cached.get("timestamp", 0))) / 3600.0 if cached else 9999.0

    if not force_refresh and len(cached_tickers) >= 2000 and age_hours <= 24.0:
        names = cached.get("names", {})
        norm_set = {normalize_ticker(t) for t in cached_tickers if t}
        print(f"[유니버스] v12 캐시 로드: {len(norm_set):,}개 종목 ({age_hours:.1f}시간 전 갱신)")
        return sorted(norm_set), norm_set, names

    try:
        merged_names, counts = _fetch_v12_universe_standalone()
        if len(merged_names) >= 1500:
            sorted_tickers = sorted(merged_names.keys())
            save_json_file(
                UNIVERSE_CACHE_PATH,
                {
                    "timestamp": now_ts,
                    "updated_kst": datetime.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                    "counts": counts,
                    "tickers": sorted_tickers,
                    "names": merged_names,
                },
            )
            print(
                f"[유니버스] 경량 라이브 구축 완료: "
                f"S&P500={counts['SP500']} / NDX100={counts['NDX100']} / R3000={counts['R3000']} -> 총 {len(sorted_tickers):,}개"
            )
            return sorted_tickers, set(sorted_tickers), merged_names
    except Exception as e:
        print(f"[경고] 경량 유니버스 구축 오류: {e}")

    bot_v12_path = os.path.join(BASE_DIR, "us_stock_bot.py")
    if os.path.exists(bot_v12_path):
        try:
            spec = importlib.util.spec_from_file_location("us_stock_bot_v12", bot_v12_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            tickers, names, _counts = mod.build_index_universe(force_refresh=force_refresh)
            norm_set = {normalize_ticker(t) for t in tickers if t}
            return sorted(norm_set), norm_set, names
        except Exception as e:
            print(f"[경고] v12 유니버스 동적 갱신 실패 (기존 캐시 폴백): {e}")

    if cached_tickers:
        norm_set = {normalize_ticker(t) for t in cached_tickers if t}
        return sorted(norm_set), norm_set, cached.get("names", {})
    raise RuntimeError("유니버스 구축 및 us_universe_cache.json 로드에 모두 실패했습니다.")


def load_sec_cik_map(universe_set: set, sec_user_agent: str, force_refresh: bool = False):
    """
    SEC company_tickers.json을 조회하여 v12 유니버스에 해당하는 CIK <-> Ticker 매핑을 구축합니다.
    24시간 로컬 캐시를 사용합니다.
    """
    now_ts = time.time()
    cached = load_json_file(SEC_CIK_CACHE_PATH, {})
    age_hours = (now_ts - float(cached.get("timestamp", 0))) / 3600.0 if cached else 9999.0

    raw_rows = cached.get("rows")
    if force_refresh or not raw_rows or age_hours > 24.0:
        try:
            sess = get_thread_cf_session()
            r = sess.get(
                "https://www.sec.gov/files/company_tickers.json",
                headers={"User-Agent": sec_user_agent, "Accept": "application/json"},
                timeout=20,
            )
            if r.status_code == 200:
                data = r.json()
                raw_rows = list(data.values())
                save_json_file(SEC_CIK_CACHE_PATH, {"timestamp": now_ts, "rows": raw_rows})
                print(f"[SEC 매핑] company_tickers.json 최신 로드 완료 ({len(raw_rows):,}개 엔티티)")
        except Exception as e:
            print(f"[경고] SEC CIK 매핑 갱신 실패 (기존 캐시 사용): {e}")

    cik_to_ticker: Dict[str, str] = {}
    ticker_to_cik: Dict[str, str] = {}
    if raw_rows:
        for item in raw_rows:
            sym = normalize_ticker(item.get("ticker", ""))
            if sym not in universe_set:
                continue
            cik_str = str(item.get("cik_str", "")).zfill(10)
            if not cik_str.strip("0"):
                continue
            ticker_to_cik[sym] = cik_str
            # 동일 CIK에 복수 주식종류(예: GOOG/GOOGL)가 있을 경우 첫 번째 또는 더 짧은 기본 티커 우선 유지
            if cik_str not in cik_to_ticker or len(sym) < len(cik_to_ticker[cik_str]):
                cik_to_ticker[cik_str] = sym

    print(f"[SEC 매핑] v12 유니버스 매칭 완료: {len(ticker_to_cik):,}개 티커 / {len(cik_to_ticker):,}개 CIK")
    return cik_to_ticker, ticker_to_cik


_korean_names_mem: Optional[Dict[str, str]] = None


def get_korean_stock_name(ticker: str, universe_names: Optional[Dict[str, str]] = None) -> str:
    """
    Naver 증권 자동완성 API(ac.stock.naver.com)로 미국 종목의 한글명을 조회하고 영구 캐시합니다.
    조회 불가 시 v12 유니버스 영문명을 정제하여 폴백합니다.
    """
    global _korean_names_mem
    norm = normalize_ticker(ticker)
    with _cache_lock:
        if _korean_names_mem is None:
            _korean_names_mem = load_json_file(KOREAN_NAMES_CACHE_PATH, {})
        if norm in _korean_names_mem and _korean_names_mem[norm]:
            return _korean_names_mem[norm]

    sess = get_thread_cf_session()
    found_name = None
    candidates_q = [norm]
    if "-" in norm:
        candidates_q.append(norm.replace("-", "."))

    for q in candidates_q:
        try:
            r = sess.get(
                "https://ac.stock.naver.com/ac",
                params={"q": q, "target": "stock"},
                timeout=8,
            )
            if r.status_code != 200:
                continue
            items = r.json().get("items") or []
            for it in items:
                if it.get("nationCode") != "USA":
                    continue
                code = normalize_ticker(it.get("code", ""))
                if code == norm:
                    nm = str(it.get("name") or "").strip()
                    if nm:
                        found_name = nm
                        break
            if found_name:
                break
        except Exception:
            pass

    if found_name:
        with _cache_lock:
            _korean_names_mem[norm] = found_name
        save_json_file(KOREAN_NAMES_CACHE_PATH, _korean_names_mem)
        return found_name

    fallback = (universe_names or {}).get(norm) or (universe_names or {}).get(ticker) or norm
    return str(fallback).strip()


# =====================================================================
# 4. Phase 0: 장전 컨센서스 스냅샷 & 주요 실적 일정 브리핑
# =====================================================================
def _parse_money_str_to_usd(val: Any) -> float:
    if val is None:
        return 0.0
    s = str(val).replace("$", "").replace(",", "").strip().upper()
    if not s or s in ("N/A", "--", "-"):
        return 0.0
    mult = 1.0
    if s.endswith("T"):
        mult = 1e12
        s = s[:-1]
    elif s.endswith("B"):
        mult = 1e9
        s = s[:-1]
    elif s.endswith("M"):
        mult = 1e6
        s = s[:-1]
    try:
        return float(s) * mult
    except Exception:
        return 0.0


def _parse_signed_dollar_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = str(val).replace("$", "").replace(",", "").strip()
    if not s or s in ("N/A", "--", "-"):
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()
    try:
        num = float(s)
        return -num if neg else num
    except Exception:
        return None


def fetch_nasdaq_earnings_calendar(date_str: str) -> List[Dict[str, Any]]:
    """Nasdaq Calendar API에서 특정 일자(YYYY-MM-DD)의 미국 실적발표 예정 목록을 수집합니다."""
    sess = get_thread_cf_session()
    headers = {
        "Accept": "application/json",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
    }
    try:
        r = sess.get(
            f"https://api.nasdaq.com/api/calendar/earnings?date={date_str}",
            headers=headers,
            timeout=15,
        )
        if r.status_code != 200:
            print(f"[경고] Nasdaq 캘린더 HTTP {r.status_code} ({date_str})")
            return []
        rows = (r.json().get("data") or {}).get("rows") or []
    except Exception as e:
        print(f"[경고] Nasdaq 캘린더 수집 실패 ({date_str}): {e}")
        return []

    out = []
    for row in rows:
        sym = normalize_ticker(row.get("symbol", ""))
        if not sym:
            continue
        t_raw = str(row.get("time") or "").lower()
        if "pre-market" in t_raw:
            session = "BMO"
        elif "after-hours" in t_raw:
            session = "AMC"
        else:
            session = "TBD"

        try:
            ests = int(str(row.get("noOfEsts") or "0").replace(",", "").strip() or 0)
        except Exception:
            ests = 0

        out.append({
            "ticker": sym,
            "name": str(row.get("name") or sym).strip(),
            "session": session,
            "market_cap_usd": _parse_money_str_to_usd(row.get("marketCap")),
            "fiscal_quarter_ending": str(row.get("fiscalQuarterEnding") or "").strip(),
            "nasdaq_eps_forecast": _parse_signed_dollar_float(row.get("epsForecast")),
            "nasdaq_num_ests": ests,
        })
    return out


_yahoo_crumb_lock = threading.Lock()
_shared_yahoo_sess = None
_shared_yahoo_crumb = None


def get_yahoo_session_and_crumb(force_refresh: bool = False):
    """스레드 안전한 Yahoo Finance 쿠키 + Crumb 반환."""
    global _shared_yahoo_sess, _shared_yahoo_crumb
    with _yahoo_crumb_lock:
        if _shared_yahoo_sess is None or force_refresh or not _shared_yahoo_crumb:
            s = cf_requests.Session(impersonate="chrome", verify=False)
            try:
                s.get("https://fc.yahoo.com", timeout=10)
                r = s.get("https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=10)
                if r.status_code == 200 and r.text and "<" not in r.text:
                    _shared_yahoo_sess = s
                    _shared_yahoo_crumb = r.text.strip()
            except Exception:
                pass
        return _shared_yahoo_sess or get_thread_cf_session(), _shared_yahoo_crumb


def _raw_val(obj: Any) -> Optional[float]:
    if isinstance(obj, dict):
        v = obj.get("raw")
        return float(v) if v is not None else None
    if isinstance(obj, (int, float)):
        return float(obj)
    return None


def fetch_yahoo_consensus_bundle(ticker: str) -> Dict[str, Any]:
    """
    Yahoo Finance v10 quoteSummary에서 earningsTrend(0q, +1q, 0y, +1y),
    earningsHistory, financialsChart(quarterly)를 일괄 수집합니다.
    """
    yf_sym = normalize_ticker(ticker)
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{yf_sym}"
    bundle: Dict[str, Any] = {
        "ticker": yf_sym,
        "trend": {},
        "history": [],
        "recent_financials": [],
    }

    for attempt in range(2):
        sess, crumb = get_yahoo_session_and_crumb(force_refresh=(attempt > 0))
        params = {"modules": "earningsTrend,earningsHistory,earnings,financialData"}
        if crumb:
            params["crumb"] = crumb
        try:
            r = sess.get(url, params=params, timeout=12)
            if r.status_code in (401, 403):
                get_yahoo_session_and_crumb(force_refresh=True)
                continue
            if r.status_code != 200:
                return bundle

            res0 = (r.json().get("quoteSummary", {}).get("result") or [None])[0]
            if not res0:
                return bundle

            for t_item in (res0.get("earningsTrend") or {}).get("trend") or []:
                period = t_item.get("period")
                if not period:
                    continue
                ee = t_item.get("earningsEstimate") or {}
                re_est = t_item.get("revenueEstimate") or {}
                rev_raw = _raw_val(re_est.get("avg"))
                bundle["trend"][period] = {
                    "end_date": t_item.get("endDate"),
                    "eps_est": _raw_val(ee.get("avg")),
                    "num_eps_analysts": int(_raw_val(ee.get("numberOfAnalysts")) or 0),
                    "rev_est_musd": (rev_raw / 1e6) if rev_raw is not None else None,
                    "num_rev_analysts": int(_raw_val(re_est.get("numberOfAnalysts")) or 0),
                }

            for h_item in (res0.get("earningsHistory") or {}).get("history") or []:
                q_fmt = (h_item.get("quarter") or {}).get("fmt")
                bundle["history"].append({
                    "quarter_end": q_fmt,
                    "eps_est": _raw_val(h_item.get("epsEstimate")),
                    "eps_actual": _raw_val(h_item.get("epsActual")),
                    "surprise_pct": _raw_val(h_item.get("surprisePercent")),
                })

            fc_list = ((res0.get("earnings") or {}).get("financialsChart") or {}).get("quarterly") or []
            for fc in fc_list:
                rev_raw = _raw_val(fc.get("revenue"))
                earn_raw = _raw_val(fc.get("earnings"))
                bundle["recent_financials"].append({
                    "date": fc.get("date"),
                    "fiscal_quarter": fc.get("fiscalQuarter"),
                    "rev_musd": (rev_raw / 1e6) if rev_raw is not None else None,
                    "earnings_musd": (earn_raw / 1e6) if earn_raw is not None else None,
                })
            return bundle
        except Exception:
            if attempt == 0:
                time.sleep(0.5)
    return bundle


def build_daily_consensus_snapshot(
    et_date_str: str,
    universe_set: set,
    universe_names: Dict[str, str],
    force: bool = False,
) -> Dict[str, Any]:
    """
    당일 실적발표 예정 v12 전종목의 장전 컨센서스(0q, +1q, 0y, +1y)를 병렬 조회하여
    daily_consensus_snapshot.json에 고정 저장(롤오버 유실 방지)합니다.
    """
    all_snaps = load_json_file(SNAPSHOT_PATH, {})
    if not force and et_date_str in all_snaps and all_snaps[et_date_str].get("items"):
        return all_snaps[et_date_str]

    print(f"[Phase 0] {et_date_str} 미국 실적 일정 및 장전 컨센서스 스냅샷 구축 중...")
    cal_rows = fetch_nasdaq_earnings_calendar(et_date_str)
    matched = [row for row in cal_rows if row["ticker"] in universe_set]
    print(f"[Phase 0] Nasdaq 일정 {len(cal_rows)}건 중 v12 유니버스 매칭: {len(matched)}개 종목")

    # Crumb 사전 워밍업
    get_yahoo_session_and_crumb()

    items: Dict[str, Any] = {}

    def _enrich(row: Dict[str, Any]):
        sym = row["ticker"]
        yb = fetch_yahoo_consensus_bundle(sym)
        t0 = yb["trend"].get("0q") or {}
        has_cons = any([
            t0.get("eps_est") is not None,
            t0.get("rev_est_musd") is not None,
            row.get("nasdaq_eps_forecast") is not None,
        ])
        return sym, {
            **row,
            "trend": yb["trend"],
            "has_consensus": has_cons,
        }

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(_enrich, r) for r in matched]
        for fut in as_completed(futs):
            try:
                sym, info = fut.result()
                items[sym] = info
            except Exception:
                pass

    # 주요 종목 한글명 사전 캐싱
    major_rows = sorted(items.values(), key=lambda x: x.get("market_cap_usd", 0.0), reverse=True)[:35]
    for r in major_rows:
        if r.get("has_consensus"):
            get_korean_stock_name(r["ticker"], universe_names)

    day_snap = {
        "et_date": et_date_str,
        "built_at_kst": datetime.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "total_matched": len(items),
        "with_consensus": sum(1 for v in items.values() if v.get("has_consensus")),
        "items": items,
    }

    # 최근 3일분만 유지하여 저장소 상태 파일 비대화 방지
    all_snaps[et_date_str] = day_snap
    sorted_dates = sorted(all_snaps.keys())[-3:]
    pruned = {d: all_snaps[d] for d in sorted_dates}
    save_json_file(SNAPSHOT_PATH, pruned)
    print(
        f"[Phase 0] 스냅샷 저장 완료: 총 {day_snap['total_matched']}종목 "
        f"(컨센서스 보유 {day_snap['with_consensus']}종목)"
    )
    return day_snap


def format_musd(val_musd: Optional[float]) -> str:
    """USD millions 값을 가독성 있는 $B / $M 문자열(소수점 한 자리)로 변환합니다."""
    if val_musd is None:
        return "-"
    sign = "-" if val_musd < 0 else ""
    av = abs(val_musd)
    if av >= 1000.0:
        return f"{sign}${av / 1000.0:.1f}B"
    return f"{sign}${av:.1f}M"


def format_eps(val: Optional[float]) -> str:
    if val is None:
        return "-"
    if val < 0:
        return f"-${abs(val):.2f}"
    return f"${val:.2f}"


def build_major_schedule_message(
    et_date_str: str,
    day_snap: Dict[str, Any],
    universe_names: Dict[str, str],
    config: Dict[str, Any],
) -> Optional[str]:
    """
    당일 주요 실적발표 예정 종목 리스트(BMO / AMC 구분, 컨센서스 보유 종목만)를 생성합니다.
    """
    items = list((day_snap.get("items") or {}).values())
    cons_items = [x for x in items if x.get("has_consensus")]
    if not cons_items:
        return None

    min_mcap = float(config.get("major_schedule_min_mcap_usd", 10_000_000_000))
    max_per_sess = int(config.get("major_schedule_max_per_session", 15))

    major = [x for x in cons_items if float(x.get("market_cap_usd") or 0) >= min_mcap]
    if len(major) < 5:
        major = sorted(cons_items, key=lambda x: float(x.get("market_cap_usd") or 0), reverse=True)[:12]

    bmo = sorted([x for x in major if x.get("session") == "BMO"], key=lambda x: x.get("market_cap_usd", 0), reverse=True)[:max_per_sess]
    amc = sorted([x for x in major if x.get("session") == "AMC"], key=lambda x: x.get("market_cap_usd", 0), reverse=True)[:max_per_sess]
    tbd = sorted([x for x in major if x.get("session") == "TBD"], key=lambda x: x.get("market_cap_usd", 0), reverse=True)[:max_per_sess]

    if not (bmo or amc or tbd):
        return None

    def _fmt_row(it: Dict[str, Any]) -> str:
        sym = it["ticker"]
        kr = html.escape(get_korean_stock_name(sym, universe_names))
        t0 = (it.get("trend") or {}).get("0q") or {}
        rev_c = t0.get("rev_est_musd")
        eps_c = t0.get("eps_est") if t0.get("eps_est") is not None else it.get("nasdaq_eps_forecast")
        parts = []
        if rev_c is not None:
            parts.append(f"매출 {format_musd(rev_c)}")
        if eps_c is not None:
            parts.append(f"EPS {format_eps(eps_c)}")
        cons_str = " / ".join(parts) if parts else "컨센 확인중"
        return f"-<b>{sym}({kr})</b> 컨센 {cons_str}"

    lines = [f"<b>미국 주요 실적발표 일정 ({et_date_str})</b>"]
    if bmo:
        lines.append("")
        lines.append("<b>장전 발표 (BMO)</b>")
        lines.extend(_fmt_row(x) for x in bmo)
    if amc:
        lines.append("")
        lines.append("<b>장후 발표 (AMC)</b>")
        lines.extend(_fmt_row(x) for x in amc)
    if tbd:
        lines.append("")
        lines.append("<b>시간 미정 (TBD)</b>")
        lines.extend(_fmt_row(x) for x in tbd)

    return "\n".join(lines)


# =====================================================================
# 5. Phase 1: 실시간 SEC EDGAR 8-K (Item 2.02) & 6-K 수집기
# =====================================================================
def _sec_get(url: str, sec_user_agent: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15):
    sess = get_thread_cf_session()
    headers = {"User-Agent": sec_user_agent, "Accept-Encoding": "gzip, deflate"}
    for attempt in range(3):
        try:
            r = sess.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 429:
                time.sleep(1.5 * (attempt + 1))
                continue
            return r
        except Exception:
            if attempt < 2:
                time.sleep(0.5)
    return None


def poll_sec_filings(
    cik_to_ticker: Dict[str, str],
    seen_cache: set,
    et_date_str: str,
    sec_user_agent: str,
) -> List[Dict[str, Any]]:
    """
    SEC EDGAR 실시간 Atom 피드(8-K, 6-K) + EFTS 검색 백스톱(8-K Item 2.02)을 결합하여
    미처리된 신규 실적 공시 후보를 반환합니다.
    """
    discovered: Dict[str, Dict[str, Any]] = {}

    # Track 1: SEC 실시간 Atom Feed (초단위 지연)
    for form in ("8-K", "6-K"):
        cnt = 100 if form == "8-K" else 60
        url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
            f"&type={form}&company=&dateb=&owner=include&start=0&count={cnt}&output=atom"
        )
        r = _sec_get(url, sec_user_agent)
        if not r or r.status_code != 200:
            continue

        soup = BeautifulSoup(r.text, "xml")
        for entry in soup.find_all("entry"):
            title = entry.title.get_text(strip=True) if entry.title else ""
            id_txt = entry.id.get_text(strip=True) if entry.id else ""
            summary_txt = entry.summary.get_text(" ", strip=True) if entry.summary else ""
            updated_txt = entry.updated.get_text(strip=True) if entry.updated else ""
            link_tag = entry.find("link")
            link_href = link_tag.get("href", "") if link_tag else ""

            if form == "8-K" and "2.02" not in summary_txt:
                continue

            m_acc = re.search(r"accession-number=(\d{10}-\d{2}-\d{6})", id_txt)
            if not m_acc:
                m_acc = re.search(r"/(\d{10}-\d{2}-\d{6})-index", link_href)
            if not m_acc:
                continue
            acc = m_acc.group(1)

            m_cik = re.search(r"\((\d{10})\)", title)
            if not m_cik:
                m_cik = re.search(r"/data/(\d+)/", link_href)
            if not m_cik:
                continue
            cik = m_cik.group(1).zfill(10)

            if cik not in cik_to_ticker:
                continue
            ticker = cik_to_ticker[cik]

            if acc in seen_cache or f"SENT:{et_date_str}:{ticker}" in seen_cache:
                continue

            discovered[acc] = {
                "accession": acc,
                "cik": cik,
                "ticker": ticker,
                "form": form,
                "filing_date": updated_txt[:10] if updated_txt else et_date_str,
                "source": "atom",
            }

    # Track 2: SEC EFTS 8-K Item 2.02 백스톱 (피크 타임 누락 방지)
    et_dt = datetime.datetime.strptime(et_date_str, "%Y-%m-%d").date()
    prev_dt = (et_dt - datetime.timedelta(days=1)).isoformat()
    r_efts = _sec_get(
        "https://efts.sec.gov/LATEST/search-index",
        sec_user_agent,
        params={
            "forms": "8-K",
            "q": '"2.02"',
            "dateRange": "custom",
            "startdt": prev_dt,
            "enddt": et_date_str,
        },
    )
    if r_efts and r_efts.status_code == 200:
        try:
            hits = (r_efts.json().get("hits") or {}).get("hits") or []
            for h in hits:
                src = h.get("_source") or {}
                items = [str(x) for x in (src.get("items") or [])]
                if "2.02" not in items:
                    continue
                acc = str(h.get("_id") or "").split(":")[0]
                if not re.fullmatch(r"\d{10}-\d{2}-\d{6}", acc):
                    continue
                ciks = [str(c).zfill(10) for c in (src.get("ciks") or [])]
                matched_cik = next((c for c in ciks if c in cik_to_ticker), None)
                if not matched_cik:
                    continue
                ticker = cik_to_ticker[matched_cik]
                f_date = str(src.get("file_date") or et_date_str)[:10]
                if acc in seen_cache or f"SENT:{f_date}:{ticker}" in seen_cache or f"SENT:{et_date_str}:{ticker}" in seen_cache:
                    continue
                if acc not in discovered:
                    discovered[acc] = {
                        "accession": acc,
                        "cik": matched_cik,
                        "ticker": ticker,
                        "form": "8-K",
                        "filing_date": f_date,
                        "source": "efts",
                    }
        except Exception:
            pass

    return list(discovered.values())


def find_latest_sec_earnings_filing(cik: str, sec_user_agent: str) -> Optional[Dict[str, Any]]:
    """단일 종목(--test-ticker) 검증용: 최근 SEC submissions에서 최신 8-K(Item 2.02) 또는 실적 6-K를 탐색합니다."""
    cik_pad = str(cik).zfill(10)
    r = _sec_get(f"https://data.sec.gov/submissions/CIK{cik_pad}.json", sec_user_agent, timeout=15)
    if not r or r.status_code != 200:
        return None
    rec = (r.json().get("filings") or {}).get("recent") or {}
    forms = rec.get("form") or []
    accs = rec.get("accessionNumber") or []
    dates = rec.get("filingDate") or []
    items_list = rec.get("items") or []

    for i, form in enumerate(forms):
        if form in ("8-K", "8-K/A") and "2.02" in str(items_list[i]):
            return {
                "accession": accs[i],
                "cik": cik_pad,
                "form": form,
                "filing_date": dates[i],
            }
    for i, form in enumerate(forms):
        if form in ("6-K", "6-K/A"):
            return {
                "accession": accs[i],
                "cik": cik_pad,
                "form": form,
                "filing_date": dates[i],
            }
    return None


def _html_to_structured_text(raw_html: str) -> str:
    """HTML 보도자료에서 스크립트를 제거하고 표 행 구조를 ' | ' 구분자로 보존한 텍스트를 반환합니다."""
    soup = BeautifulSoup(raw_html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # 테이블 행을 한 줄로 정리해 숫자와 항목 라벨이 분리되지 않도록 보존
    for tr in soup.find_all("tr"):
        cells = []
        for cell in tr.find_all(["th", "td"]):
            t = re.sub(r"\s+", " ", cell.get_text(" ", strip=True)).strip()
            if t:
                cells.append(t)
        if cells:
            tr.replace_with(soup.new_string("\n" + " | ".join(cells) + "\n"))

    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\n{2,}", "\n", text)
    return text


def fetch_sec_earnings_exhibits(
    cik: str,
    accession: str,
    sec_user_agent: str,
    form_type: str = "8-K",
) -> Dict[str, Any]:
    """
    SEC 공시 인덱스에서 EX-99.1 (필요 시 EX-99.2 CFO 코멘터리 포함) HTML 본문을 추출합니다.
    """
    cik_int = str(int(cik))
    acc_nodash = accession.replace("-", "")
    idx_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{accession}-index.html"
    r_idx = _sec_get(idx_url, sec_user_agent)
    if not r_idx or r_idx.status_code != 200:
        return {"text": "", "url": idx_url, "exhibits": []}

    soup = BeautifulSoup(r_idx.text, "html.parser")
    candidates = []
    for tr in soup.select("table.tableFile tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        desc = tds[1].get_text(" ", strip=True).lower()
        a_tag = tds[2].find("a")
        fname = a_tag.get_text(strip=True) if a_tag else tds[2].get_text(strip=True)
        fname = fname.split()[0].strip()
        ex_type = tds[3].get_text(" ", strip=True).upper()
        href = a_tag.get("href", "") if a_tag else ""
        if href.startswith("/ix?doc="):
            href = href.replace("/ix?doc=", "", 1)
        if not fname.lower().endswith((".htm", ".html")) or "index" in fname.lower():
            continue
        doc_url = f"https://www.sec.gov{href}" if href.startswith("/") else f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{fname}"
        candidates.append({"type": ex_type, "desc": desc, "fname": fname, "url": doc_url})

    selected = []
    # 1순위: EX-99.1
    for c in candidates:
        if c["type"].startswith("EX-99.1") or "99-1" in c["fname"].lower() or "991" in c["fname"].lower():
            selected.append(c)
            break

    # 2순위: EX-99.2가 CFO 코멘터리/보도자료인 경우 함께 포함
    for c in candidates:
        if c in selected:
            continue
        if c["type"].startswith("EX-99.2") and any(k in (c["desc"] + c["fname"].lower()) for k in ("cfo", "commentary", "earn", "release", "financial", "supplement")):
            selected.append(c)
            break

    # 폴백: 일반 EX-99 또는 6-K 본문
    if not selected:
        ex99s = [c for c in candidates if "EX-99" in c["type"]]
        if ex99s:
            selected.append(ex99s[0])
        elif form_type.startswith("6-K") and candidates:
            selected.append(candidates[0])
        elif candidates:
            selected.append(candidates[0])

    texts = []
    used_urls = []
    for c in selected[:2]:
        r_doc = _sec_get(c["url"], sec_user_agent, timeout=20)
        if r_doc and r_doc.status_code == 200:
            cleaned = _html_to_structured_text(r_doc.text)
            if cleaned:
                texts.append(f"[{c['type']} - {c['fname']}]\n{cleaned}")
                used_urls.append(c["url"])

    combined = "\n\n".join(texts)
    if len(combined) > 48000:
        combined = combined[:48000]

    return {
        "text": combined,
        "url": used_urls[0] if used_urls else idx_url,
        "exhibits": used_urls,
    }


def is_6k_earnings_document(text: str) -> bool:
    """6-K 공시가 단순 IR 일정/주주총회 공지가 아닌 실제 실적 발표인지 사전 판별합니다."""
    low = (text or "").lower()
    has_rev = "revenue" in low
    has_earn_kw = any(k in low for k in ("earnings", "financial results", "quarter", "diluted", "operating income", "eps"))
    return has_rev and has_earn_kw


# =====================================================================
# 6. Phase 2: Method B (Gemini Structured Output + Grounded Search + 폴백)
# =====================================================================
def _get_gemini_client(api_key: str, max_attempts: int = 2):
    if not HAS_GENAI or not api_key:
        return None
    return genai.Client(
        api_key=api_key,
        http_options=genai_types.HttpOptions(
            client_args={"verify": False},
            retry_options=genai_types.HttpRetryOptions(
                attempts=max_attempts,
                initial_delay=1.0,
                max_delay=3.0,
            ),
        ),
    )


def extract_earnings_with_gemini(
    ticker: str,
    filing_date: str,
    exhibit_text: str,
    config: Dict[str, Any],
) -> Optional[EarningsExtraction]:
    """
    Gemini API Interactions (`gemini-4-hydrogen`) Structured Output으로
    SEC Exhibit 99.1/99.2 본문에서 실적 수치와 가이던스 밴드를 정밀 추출합니다.
    """
    api_key = config.get("gemini_api_key", "")
    client = _get_gemini_client(api_key)
    if client is None or not exhibit_text:
        return None

    prompt = f"""You are a financial earnings parser analyzing an official SEC filing exhibit for ticker {ticker} (filing date: {filing_date}).

TASK:
Extract the reported quarterly actuals and forward guidance strictly from the SEC text below. NEVER guess or invent any number not explicitly written in the text.

CRITICAL RULES:
1. Units:
   - Express ALL monetary figures (revenue, capex) in USD MILLIONS (e.g. "$115.8 billion" -> 115800.0; "$3.44 billion" -> 3440.0; "$96 million" -> 96.0).
   - Express ALL margins (GPM = gross margin, OPM = operating margin) as percentages (e.g. "74.8%" -> 74.8).
   - Express EPS in USD per share (diluted).
2. Non-GAAP preference:
   - For actuals (eps_actual, gpm_pct, opm_pct) and guidance, PREFER Non-GAAP / Adjusted figures when explicitly reported (because Wall Street consensus is Non-GAAP). If Non-GAAP is not reported for a metric, use GAAP.
   - If Operating Margin % (opm_pct) is not written as a percentage, compute it ONLY if both Operating Income (prefer Non-GAAP if Non-GAAP EPS is used, else GAAP) and Revenue are explicitly given in the same basis.
3. Guidance ranges & plus/minus bands:
   - If a guidance metric is stated as a range "$A to $B" or "between X% and Y%", populate `low` and `high`.
   - If stated as "$130.0 billion, plus or minus 3%", compute `low` = 130000 * 0.97 = 126100.0 and `high` = 130000 * 1.03 = 133900.0.
   - If stated as "74.5%, plus or minus 50 basis points", compute `low` = 74.0 and `high` = 75.0.
   - If stated as a single approximate number ("approximately $X"), populate `point` and leave `low`/`high` null.
   - Do NOT emit guidance for an item if the company did not provide guidance for that item (leave null).
4. Separate `next_quarter_guidance` (next fiscal quarter outlook) and `full_year_guidance` (current or next full fiscal year outlook). Set `has_guidance=true` only when at least one of revenue/eps/capex/gpm/opm is guided for that period.
5. Set `is_earnings_release=false` if this filing is NOT an earnings release (e.g. M&A, bond offering, executive change).

SEC EXHIBIT TEXT:
{exhibit_text}
"""
    try:
        interaction = client.interactions.create(
            model=config.get("gemini_model", "gemini-4-hydrogen"),
            input=prompt,
            store=False,
            generation_config={"thinking_level": "low"},
            timeout=35.0,
            response_format={
                "type": "text",
                "mime_type": "application/json",
                "schema": EarningsExtraction.model_json_schema(),
            },
        )
        raw_out = interaction.output_text
        if not raw_out:
            return None
        return EarningsExtraction.model_validate_json(raw_out)
    except Exception as e:
        print(f"[경고] Gemini 구조화 추출 오류 ({ticker}): {e}")
        return None


def fetch_grounded_supplemental_consensus(
    ticker: str,
    extraction: EarningsExtraction,
    config: Dict[str, Any],
) -> SupplementalConsensus:
    """
    가이던스에 CAPEX / GPM / OPM이 포함된 경우에 한해, Gemini + Google Search 그라운딩으로
    월가 CAPEX·마진 컨센서스를 보조 조회합니다. 검증 불가능한 수치는 반드시 null을 반환합니다.
    """
    empty = SupplementalConsensus()
    if not config.get("enable_grounded_margin_capex_consensus", True):
        return empty

    nq = extraction.next_quarter_guidance
    fy = extraction.full_year_guidance
    need_nq = bool(nq and nq.has_guidance and (nq.capex or nq.gpm or nq.opm))
    need_fy = bool(fy and fy.has_guidance and (fy.capex or fy.gpm or fy.opm))
    if not (need_nq or need_fy):
        return empty

    client = _get_gemini_client(config.get("gemini_api_key", ""), max_attempts=1)
    if client is None:
        return empty

    nq_lbl = nq.period_label if (nq and nq.period_label) else "next quarter"
    fy_lbl = fy.period_label if (fy and fy.period_label) else "current/next fiscal year"

    prompt = f"""Find the pre-release Wall Street consensus analyst estimates (Bloomberg / FactSet / Visible Alpha / StreetAccount) for ticker {ticker}:
- Next quarter ({nq_lbl}): CAPEX consensus (USD millions), Gross Margin % consensus, Operating Margin % consensus.
- Full year ({fy_lbl}): CAPEX consensus (USD millions), Gross Margin % consensus, Operating Margin % consensus.

STRICT RULE:
Only fill a field if an exact Wall Street consensus estimate is explicitly verifiable in the search results. If not explicitly found, return null. Never fabricate or extrapolate."""

    try:
        interaction = client.interactions.create(
            model=config.get("gemini_search_model", "gemini-4-hydrogen"),
            input=prompt,
            tools=[{"type": "google_search"}],
            store=False,
            generation_config={"thinking_level": "low"},
            timeout=25.0,
            response_format={
                "type": "text",
                "mime_type": "application/json",
                "schema": SupplementalConsensus.model_json_schema(),
            },
        )
        if interaction.output_text:
            return SupplementalConsensus.model_validate_json(interaction.output_text)
    except Exception as e:
        print(f"[참고] 보조 컨센서스 검색 생략/폴백 ({ticker}): {e}")
    return empty


def _unit_to_musd(val_str: str, unit_str: Optional[str]) -> float:
    num = float(val_str.replace(",", ""))
    u = (unit_str or "").lower()
    if u.startswith("b"):
        return num * 1000.0
    if u.startswith("t"):
        return num * 1_000_000.0
    return num


def _parse_guidance_block_regex(block: str, period_label: Optional[str] = None, fiscal_year: Optional[int] = None) -> PeriodGuidance:
    g = PeriodGuidance(has_guidance=False, period_label=period_label, fiscal_year=fiscal_year)
    if not block:
        return g

    # 1. Revenue range: "$3.530 billion to $3.550 billion" or "$130.0 billion, plus or minus 3%"
    m_rev_pm = re.search(
        r"revenue[^.\n]{0,80}?\$(\d[\d,]*\.?\d*)\s*(billion|million)\s*,?\s*(?:plus or minus|\+/-)\s*(\d+(?:\.\d+)?)\s*%",
        block,
        re.IGNORECASE,
    )
    m_rev_rng = re.search(
        r"revenue[^.\n]{0,80}?(?:of|between|range of)?\s*\$(\d[\d,]*\.?\d*)\s*(billion|million)?\s*(?:to|and|-|–)\s*\$(\d[\d,]*\.?\d*)\s*(billion|million)",
        block,
        re.IGNORECASE,
    )
    if m_rev_pm:
        mid = _unit_to_musd(m_rev_pm.group(1), m_rev_pm.group(2))
        pct = float(m_rev_pm.group(3)) / 100.0
        g.revenue = MetricGuidance(low=round(mid * (1.0 - pct), 2), high=round(mid * (1.0 + pct), 2))
        g.has_guidance = True
    elif m_rev_rng:
        u2 = m_rev_rng.group(4)
        u1 = m_rev_rng.group(2) or u2
        g.revenue = MetricGuidance(
            low=_unit_to_musd(m_rev_rng.group(1), u1),
            high=_unit_to_musd(m_rev_rng.group(3), u2),
        )
        g.has_guidance = True

    # 2. EPS range (prefer Non-GAAP)
    m_eps_rng = re.search(
        r"(?:non-gaap[^.\n]{0,60}?(?:per share|eps)|(?:diluted[^.\n]{0,40}?per share|eps))[^.\n]{0,60}?\$(\d+\.\d+)\s*(?:to|and|-|–)\s*\$(\d+\.\d+)",
        block,
        re.IGNORECASE,
    )
    if m_eps_rng:
        g.eps = MetricGuidance(low=float(m_eps_rng.group(1)), high=float(m_eps_rng.group(2)))
        g.has_guidance = True

    # 3. Gross margin range
    m_gpm_rng = re.search(
        r"gross margin[s]?[^.\n]{0,80}?(?:between|range of|of)?\s*(\d+(?:\.\d+)?)\s*%?\s*(?:to|and|-|–)\s*(\d+(?:\.\d+)?)\s*%",
        block,
        re.IGNORECASE,
    )
    m_gpm_pm = re.search(
        r"gross margin[s]?[^.\n]{0,60}?(\d+(?:\.\d+)?)\s*%\s*,?\s*(?:plus or minus|\+/-)\s*(\d+(?:\.\d+)?)\s*(?:basis points|bps|%)",
        block,
        re.IGNORECASE,
    )
    if m_gpm_rng:
        g.gpm = MetricGuidance(low=float(m_gpm_rng.group(1)), high=float(m_gpm_rng.group(2)))
        g.has_guidance = True
    elif m_gpm_pm:
        mid = float(m_gpm_pm.group(1))
        delta = float(m_gpm_pm.group(2))
        if delta >= 5.0:  # bps
            delta /= 100.0
        g.gpm = MetricGuidance(low=round(mid - delta, 2), high=round(mid + delta, 2))
        g.has_guidance = True
    else:
        m_gpm_pt = re.search(
            r"gross margin[s]?[^\d%\n]{0,60}?(?:of|about|approximately|around|at)\s+(\d+(?:\.\d+)?)\s*%",
            block,
            re.IGNORECASE,
        )
        if m_gpm_pt:
            g.gpm = MetricGuidance(point=float(m_gpm_pt.group(1)))
            g.has_guidance = True

    # 4. Operating margin range or single point
    m_opm_rng = re.search(
        r"operating margin[s]?[^.\n]{0,80}?(?:between|range of|of)?\s*(\d+(?:\.\d+)?)\s*%?\s*(?:to|and|-|–)\s*(\d+(?:\.\d+)?)\s*%",
        block,
        re.IGNORECASE,
    )
    if m_opm_rng:
        g.opm = MetricGuidance(low=float(m_opm_rng.group(1)), high=float(m_opm_rng.group(2)))
        g.has_guidance = True
    else:
        m_opm_pt = re.search(
            r"operating margin[s]?[^\d%\n]{0,60}?(?:of|about|approximately|around|at)\s+(\d+(?:\.\d+)?)\s*%",
            block,
            re.IGNORECASE,
        )
        if m_opm_pt:
            g.opm = MetricGuidance(point=float(m_opm_pt.group(1)))
            g.has_guidance = True

    # 5. CAPEX range or single point
    m_cap_rng = re.search(
        r"(?:capital expenditures?|capex)[^.\n]{0,80}?(?:between|range of|of)?\s*\$(\d[\d,]*\.?\d*)\s*(billion|million)?\s*(?:to|and|-|–)\s*\$(\d[\d,]*\.?\d*)\s*(billion|million)",
        block,
        re.IGNORECASE,
    )
    m_cap_pt = re.search(
        r"(?:capital expenditures?|capex)[^.\n]{0,80}?(?:approximately|about|of|around)?\s*\$(\d[\d,]*\.?\d*)\s*(billion|million)",
        block,
        re.IGNORECASE,
    )
    if m_cap_rng:
        u2 = m_cap_rng.group(4)
        u1 = m_cap_rng.group(2) or u2
        g.capex = MetricGuidance(low=_unit_to_musd(m_cap_rng.group(1), u1), high=_unit_to_musd(m_cap_rng.group(3), u2))
        g.has_guidance = True
    elif m_cap_pt:
        g.capex = MetricGuidance(point=_unit_to_musd(m_cap_pt.group(1), m_cap_pt.group(2)))
        g.has_guidance = True

    return g


def extract_earnings_fallback(
    exhibit_text: str,
    yahoo_bundle: Optional[Dict[str, Any]] = None,
) -> EarningsExtraction:
    """
    GEMINI_API_KEY 미설정 또는 API 오류 시 작동하는 결정론적 정규식 + Yahoo 정형 보조 파서.
    """
    txt = exhibit_text or ""
    actuals = QuarterActuals()

    # Outlook 블록을 먼저 분리하여 가이던스 수치가 당기 실적 마진으로 오인되지 않도록 차단
    m_out = re.search(r"(?:financial\s+)?outlook", txt, re.IGNORECASE)
    outlook_seg = ""
    txt_no_outlook = txt
    if m_out:
        raw_out = txt[m_out.start():m_out.start() + 4500]
        cut = re.search(r"\n(?:Highlights|Earnings Call|Forward-Looking|About\s+[A-Z]|Non-GAAP\s+Measures)", raw_out)
        outlook_len = cut.start() if cut else len(raw_out)
        outlook_seg = raw_out[:outlook_len]
        txt_no_outlook = txt[:m_out.start()] + "\n" + txt[m_out.start() + outlook_len:]

    # 1. Quarter end date heuristic (e.g. "ended October 25, 2026")
    q_end_date = None
    m_date = re.search(
        r"(?:quarter|three months)\s+ended\s+([A-Z][a-z]+\.?\s+\d{1,2},\s+20\d{2})",
        txt[:4000],
        re.IGNORECASE,
    )
    if m_date:
        raw_d = m_date.group(1).replace(".", "")
        for fmt in ("%B %d, %Y", "%b %d, %Y"):
            try:
                q_end_date = datetime.datetime.strptime(raw_d, fmt).strftime("%Y-%m-%d")
                break
            except Exception:
                pass

    # 2. Revenue actual
    m_rev = re.search(
        r"(?:total\s+)?revenue[^.\n]{0,90}?(?:of|was|to)\s+\$(\d[\d,]*\.?\d*)\s*(billion|million)",
        txt_no_outlook[:6000],
        re.IGNORECASE,
    )
    if m_rev:
        actuals.revenue_musd = _unit_to_musd(m_rev.group(1), m_rev.group(2))

    # 3. Non-GAAP / GAAP Diluted EPS (본문 + Reconciliation 테이블 전체 탐색)
    m_ng_tbl_eps = re.search(
        r"(?:non-gaap|adjusted)\s+(?:net\s+income|earnings)\s+per\s+(?:diluted\s+)?share(?:,\s*diluted)?\s*\|\s*(?:\$\s*\|\s*)?\$?\s*(\d+\.\d+)",
        txt_no_outlook,
        re.IGNORECASE,
    )
    m_nongaap_eps = re.search(
        r"(?:non-gaap|adjusted)[^.\n]{0,80}?(?:diluted[^.\n]{0,40}?(?:per share|eps)|eps)[^.\n]{0,40}?\$(\d+\.\d+)",
        txt_no_outlook[:15000],
        re.IGNORECASE,
    )
    m_gaap_eps = re.search(
        r"(?:diluted\s+(?:net\s+income|earnings)\s+per\s+share[^.\n]{0,40}?\$(\d+\.\d+)|\$(\d+\.\d+)\s+per\s+diluted\s+share)",
        txt_no_outlook[:10000],
        re.IGNORECASE,
    )
    if m_ng_tbl_eps:
        actuals.eps_actual = float(m_ng_tbl_eps.group(1))
        actuals.eps_basis = "Non-GAAP"
    elif m_nongaap_eps:
        actuals.eps_actual = float(m_nongaap_eps.group(1))
        actuals.eps_basis = "Non-GAAP"
    elif m_gaap_eps:
        actuals.eps_actual = float(m_gaap_eps.group(1) or m_gaap_eps.group(2))
        actuals.eps_basis = "GAAP"

    # 4. Margins (Non-GAAP preferred, Outlook 제외 영역에서만 탐색)
    m_ng_gm = re.search(r"non-gaap\s+gross\s+margin[^\d%\n]{0,40}(\d+\.\d+)\s*%", txt_no_outlook, re.IGNORECASE)
    m_gm = re.search(r"gross\s+margin[^\d%\n]{0,40}(\d+\.\d+)\s*%", txt_no_outlook, re.IGNORECASE)
    if m_ng_gm:
        actuals.gpm_pct = float(m_ng_gm.group(1))
    elif m_gm:
        actuals.gpm_pct = float(m_gm.group(1))

    m_ng_om = re.search(r"non-gaap\s+operating\s+margin[^\d%\n]{0,40}(\d+\.\d+)\s*%", txt_no_outlook, re.IGNORECASE)
    m_om = re.search(r"operating\s+margin[^\d%\n]{0,40}(\d+\.\d+)\s*%", txt_no_outlook, re.IGNORECASE)
    if m_ng_om:
        actuals.opm_pct = float(m_ng_om.group(1))
    elif m_om:
        actuals.opm_pct = float(m_om.group(1))

    # 5. Supplement missing actuals from Yahoo recent quarters & table ratios
    if yahoo_bundle:
        hist = yahoo_bundle.get("history") or []
        fc = yahoo_bundle.get("recent_financials") or []
        if actuals.eps_actual is None and hist and hist[-1].get("eps_actual") is not None:
            actuals.eps_actual = float(hist[-1]["eps_actual"])
            actuals.eps_basis = "Non-GAAP"
        if actuals.revenue_musd is None and fc and fc[-1].get("rev_musd") is not None:
            actuals.revenue_musd = float(fc[-1]["rev_musd"])
    if actuals.opm_pct is None and actuals.revenue_musd:
        m_tbl_op = re.search(r"non-gaap\s+operating\s+income\s*\|\s*(?:\$\s*\|\s*)?\$?\s*([\d,]+\.?\d*)", txt_no_outlook, re.IGNORECASE)
        if m_tbl_op:
            try:
                op_v = float(m_tbl_op.group(1).replace(",", ""))
                ratio = op_v / actuals.revenue_musd * 100.0
                if 1.0 <= ratio <= 85.0:
                    actuals.opm_pct = round(ratio, 1)
            except Exception:
                pass

    # 6. Outlook / Guidance splitting (Next Quarter vs Full Year)
    next_q_g = None
    fy_g = None
    if outlook_seg:
        m_fy = re.search(r"For\s+(?:the\s+)?fiscal\s+year\s+(20\d{2})", outlook_seg, re.IGNORECASE)
        if m_fy:
            nq_block = outlook_seg[:m_fy.start()]
            fy_block = outlook_seg[m_fy.start():]
            fy_year = int(m_fy.group(1))
            nq_res = _parse_guidance_block_regex(nq_block, period_label="Next Quarter")
            fy_res = _parse_guidance_block_regex(fy_block, period_label=f"FY{fy_year}", fiscal_year=fy_year)
            if nq_res.has_guidance:
                next_q_g = nq_res
            if fy_res.has_guidance:
                fy_g = fy_res
        else:
            nq_res = _parse_guidance_block_regex(outlook_seg, period_label="Next Quarter")
            if nq_res.has_guidance:
                next_q_g = nq_res

    return EarningsExtraction(
        is_earnings_release=True,
        quarter_end_date=q_end_date,
        actuals=actuals,
        next_quarter_guidance=next_q_g,
        full_year_guidance=fy_g,
    )


# =====================================================================
# 7. 컨센서스 매칭(롤오버 방지) & 사용자 확정 심플 포맷터
# =====================================================================
def _parse_iso_date(s: Optional[str]) -> Optional[datetime.date]:
    if not s:
        return None
    try:
        return datetime.date.fromisoformat(s[:10])
    except Exception:
        return None


def _is_anomalous_eps_est(t_entry: Dict[str, Any], ref_eps: Optional[float]) -> bool:
    """Yahoo +1q 등에서 소수 애널리스트만 집계되어 GAAP/주식분할 전 수치로 왜곡된 EPS 컨센서스를 감지합니다."""
    eps = t_entry.get("eps_est")
    n_eps = int(t_entry.get("num_eps_analysts") or 0)
    n_rev = int(t_entry.get("num_rev_analysts") or 0)
    if eps is None:
        return False
    if n_rev >= 10 and n_eps > 0 and (n_eps / n_rev) < 0.45:
        if ref_eps is not None and abs(ref_eps) > 0.05 and abs(eps) < 0.55 * abs(ref_eps):
            return True
    return False


def resolve_consensus_for_filing(
    ticker: str,
    extraction: EarningsExtraction,
    filing_date_str: str,
    day_snap: Optional[Dict[str, Any]],
    yahoo_bundle: Dict[str, Any],
    supplemental: Optional[SupplementalConsensus] = None,
) -> Dict[str, Any]:
    """
    사전 스냅샷(Phase 0)과 최신 Yahoo quoteSummary(롤오버 자동 보정)를 결합해
    현분기, 다음분기, 다음 회계연도 컨센서스를 확정합니다.
    """
    snap_item = (day_snap or {}).get("items", {}).get(ticker)
    trend = (snap_item or {}).get("trend") or yahoo_bundle.get("trend") or {}
    history = yahoo_bundle.get("history") or []

    # 스냅샷이 없는 경우(예: --test-ticker로 과거 공시 테스트) 롤오버 여부 정밀 판정
    rolled_over = False
    if not snap_item:
        t0_date = _parse_iso_date((trend.get("0q") or {}).get("end_date"))
        ref_q_date = _parse_iso_date(extraction.quarter_end_date)
        ref_f_date = _parse_iso_date(filing_date_str)
        if t0_date and ref_q_date and (t0_date - ref_q_date).days > 45:
            rolled_over = True
        elif t0_date and ref_f_date and (t0_date - ref_f_date).days > 45:
            rolled_over = True

    # 1. 당기 실적 컨센서스
    cur_rev_cons = None
    cur_eps_cons = None
    if not rolled_over:
        q0 = trend.get("0q") or {}
        cur_rev_cons = q0.get("rev_est_musd")
        cur_eps_cons = q0.get("eps_est")
        if cur_eps_cons is None and snap_item:
            cur_eps_cons = snap_item.get("nasdaq_eps_forecast")
    else:
        # 롤오버 이후에는 earningsHistory에서 직전 발표 분기의 epsEstimate 사용 (잘못된 차분기 매출 대조 방지)
        ref_q = _parse_iso_date(extraction.quarter_end_date) or _parse_iso_date(filing_date_str)
        for h in reversed(history):
            hd = _parse_iso_date(h.get("quarter_end"))
            if hd and ref_q and abs((hd - ref_q).days) <= 45:
                cur_eps_cons = h.get("eps_est")
                break
        if cur_eps_cons is None and history:
            cur_eps_cons = history[-1].get("eps_est")

    # 2. 다음 분기 가이던스 대조용 컨센서스
    nq_key = "0q" if rolled_over else "+1q"
    nq_trend = trend.get(nq_key) or {}
    nq_rev_cons = nq_trend.get("rev_est_musd")
    nq_eps_cons = None if _is_anomalous_eps_est(nq_trend, cur_eps_cons) else nq_trend.get("eps_est")

    # 3. 회계연도(연간) 가이던스 대조용 컨센서스 (fiscal_year 매칭)
    fy_rev_cons = None
    fy_eps_cons = None
    fy_g = extraction.full_year_guidance
    target_fy = fy_g.fiscal_year if fy_g else None
    matched_y = None
    if target_fy:
        for yk in ("0y", "+1y"):
            yt = trend.get(yk) or {}
            ed = str(yt.get("end_date") or "")
            if ed.startswith(str(target_fy)):
                matched_y = yt
                break
    if matched_y is None:
        matched_y = trend.get("0y") or trend.get("+1y") or {}
    fy_rev_cons = matched_y.get("rev_est_musd")
    fy_eps_cons = matched_y.get("eps_est")

    has_quarterly_consensus = (cur_rev_cons is not None) or (cur_eps_cons is not None)

    sup = supplemental or SupplementalConsensus()
    return {
        "has_consensus": has_quarterly_consensus,
        "rolled_over": rolled_over,
        "cur_rev_musd": cur_rev_cons,
        "cur_eps": cur_eps_cons,
        "nq_rev_musd": nq_rev_cons,
        "nq_eps": nq_eps_cons,
        "nq_capex_musd": sup.next_q_capex_cons_musd,
        "nq_gpm_pct": sup.next_q_gpm_cons_pct,
        "nq_opm_pct": sup.next_q_opm_cons_pct,
        "fy_rev_musd": fy_rev_cons,
        "fy_eps": fy_eps_cons,
        "fy_capex_musd": sup.fy_capex_cons_musd,
        "fy_gpm_pct": sup.fy_gpm_cons_pct,
        "fy_opm_pct": sup.fy_opm_cons_pct,
    }


def _calc_diff_pct(actual: Optional[float], cons: Optional[float]) -> Optional[float]:
    if actual is None or cons is None or abs(cons) < 1e-9:
        return None
    return (actual - cons) / abs(cons) * 100.0


def _guidance_midpoint(mg: Optional[MetricGuidance]) -> Optional[float]:
    if mg is None:
        return None
    if mg.low is not None and mg.high is not None:
        return (mg.low + mg.high) / 2.0
    if mg.point is not None:
        return mg.point
    if mg.low is not None:
        return mg.low
    return mg.high


def _format_guidance_value(mg: Optional[MetricGuidance], kind: str) -> str:
    if mg is None:
        return ""
    fmt = format_musd if kind == "money" else (format_eps if kind == "eps" else lambda v: f"{v:.1f}%")
    if mg.low is not None and mg.high is not None:
        s_low = fmt(mg.low)
        s_high = fmt(mg.high)
        if math.isclose(mg.low, mg.high, rel_tol=1e-4) or s_low == s_high:
            return s_low
        return f"{s_low}~{s_high}"
    val = _guidance_midpoint(mg)
    return fmt(val) if val is not None else ""


def _build_guidance_section(
    header_title: str,
    pg: Optional[PeriodGuidance],
    rev_cons: Optional[float],
    eps_cons: Optional[float],
    capex_cons: Optional[float],
    gpm_cons: Optional[float],
    opm_cons: Optional[float],
) -> List[str]:
    if not pg or not pg.has_guidance:
        return []

    sublines: List[str] = []

    if pg.revenue and _guidance_midpoint(pg.revenue) is not None:
        val_str = _format_guidance_value(pg.revenue, "money")
        diff = _calc_diff_pct(_guidance_midpoint(pg.revenue), rev_cons)
        suffix = f"(컨센대비 {diff:+.1f}%)" if diff is not None else ""
        sublines.append(f"-매출 {val_str}{suffix}")

    if pg.eps and _guidance_midpoint(pg.eps) is not None:
        val_str = _format_guidance_value(pg.eps, "eps")
        diff = _calc_diff_pct(_guidance_midpoint(pg.eps), eps_cons)
        suffix = f"(컨센대비 {diff:+.1f}%)" if diff is not None else ""
        sublines.append(f"-EPS {val_str}{suffix}")
    elif eps_cons is not None:
        sublines.append(f"-EPS 컨센 {format_eps(eps_cons)}")

    if pg.capex and _guidance_midpoint(pg.capex) is not None:
        val_str = _format_guidance_value(pg.capex, "money")
        diff = _calc_diff_pct(_guidance_midpoint(pg.capex), capex_cons)
        suffix = f"(컨센대비 {diff:+.1f}%)" if diff is not None else ""
        sublines.append(f"-CAPEX {val_str}{suffix}")

    if pg.gpm and _guidance_midpoint(pg.gpm) is not None:
        val_str = _format_guidance_value(pg.gpm, "pct")
        suffix = f"(컨센 {gpm_cons:.1f}%)" if gpm_cons is not None else ""
        sublines.append(f"-GPM {val_str}{suffix}")

    if pg.opm and _guidance_midpoint(pg.opm) is not None:
        val_str = _format_guidance_value(pg.opm, "pct")
        suffix = f"(컨센 {opm_cons:.1f}%)" if opm_cons is not None else ""
        sublines.append(f"-OPM {val_str}{suffix}")

    if not sublines:
        return []
    return [f"<b>{header_title}</b>"] + sublines


def build_earnings_message(
    ticker: str,
    korean_name: str,
    extraction: EarningsExtraction,
    cons: Dict[str, Any],
) -> Optional[str]:
    """
    사용자 확정 심플 양식으로 텔레그램 HTML 메시지를 조립합니다. (이모티콘 전면 배제)
    """
    if not cons.get("has_consensus"):
        return None

    act = extraction.actuals
    rev_diff = _calc_diff_pct(act.revenue_musd, cons.get("cur_rev_musd"))
    eps_diff = _calc_diff_pct(act.eps_actual, cons.get("cur_eps"))

    # 어닝서프라이즈 / 어닝미스 판정
    if eps_diff is not None and abs(eps_diff) >= 0.05:
        verdict = "어닝서프라이즈" if eps_diff > 0 else "어닝미스"
    elif rev_diff is not None:
        verdict = "어닝서프라이즈" if rev_diff >= 0 else "어닝미스"
    elif eps_diff is not None:
        verdict = "어닝서프라이즈" if eps_diff >= 0 else "어닝미스"
    else:
        return None

    lines = [
        f"<b>{ticker}({html.escape(korean_name)})</b>",
        f"<b>{verdict}</b>",
        "",
        "<b>분기실적</b>",
    ]

    if act.revenue_musd is not None:
        r_suf = f"(컨센대비 {rev_diff:+.1f}%)" if rev_diff is not None else ""
        lines.append(f"-매출 {format_musd(act.revenue_musd)}{r_suf}")
    if act.eps_actual is not None:
        e_suf = f"(컨센대비 {eps_diff:+.1f}%)" if eps_diff is not None else ""
        lines.append(f"-EPS {format_eps(act.eps_actual)}{e_suf}")
    if act.gpm_pct is not None:
        lines.append(f"-GPM {act.gpm_pct:.1f}%")
    if act.opm_pct is not None:
        lines.append(f"-OPM {act.opm_pct:.1f}%")

    nq_lines = _build_guidance_section(
        "다음분기 가이던스",
        extraction.next_quarter_guidance,
        cons.get("nq_rev_musd"),
        cons.get("nq_eps"),
        cons.get("nq_capex_musd"),
        cons.get("nq_gpm_pct"),
        cons.get("nq_opm_pct"),
    )
    if nq_lines:
        lines.append("")
        lines.extend(nq_lines)

    fy_lines = _build_guidance_section(
        "다음 회계연도 가이던스",
        extraction.full_year_guidance,
        cons.get("fy_rev_musd"),
        cons.get("fy_eps"),
        cons.get("fy_capex_musd"),
        cons.get("fy_gpm_pct"),
        cons.get("fy_opm_pct"),
    )
    if fy_lines:
        lines.append("")
        lines.extend(fy_lines)

    return "\n".join(lines)


# =====================================================================
# 8. 텔레그램 발송 & 전체 파이프라인 오케스트레이션
# =====================================================================
def send_telegram_message(text: str, config: Dict[str, Any], dry_run: bool = False) -> bool:
    if dry_run:
        print("\n" + "-" * 56)
        print("[DRY-RUN 텔레그램 출력 미리보기]")
        print(text)
        print("-" * 56 + "\n")
        return True

    token = config.get("telegram_bot_token", "")
    chat_id = config.get("telegram_chat_id", "")
    if not token or not chat_id:
        print("[오류] telegram_bot_token 또는 telegram_chat_id가 설정되지 않았습니다.")
        return False

    sess = get_thread_cf_session()
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(3):
        try:
            r = sess.post(url, data=payload, timeout=12)
            if r.status_code == 200:
                print(f"[전송 완료] {chat_id}")
                return True
            if r.status_code == 429:
                wait_s = r.json().get("parameters", {}).get("retry_after", 5)
                time.sleep(float(wait_s) + 0.5)
                continue
            print(f"[전송 실패] HTTP {r.status_code}: {r.text}")
            return False
        except Exception as e:
            print(f"[전송 예외] {e}")
            time.sleep(1.0)
    return False


def process_single_filing(
    filing: Dict[str, Any],
    universe_names: Dict[str, str],
    config: Dict[str, Any],
    seen_cache: Optional[set] = None,
    dry_run: bool = False,
) -> bool:
    ticker = filing["ticker"]
    acc = filing["accession"]
    cik = filing["cik"]
    form = filing.get("form", "8-K")
    f_date = filing.get("filing_date") or datetime.datetime.now(ET).strftime("%Y-%m-%d")

    ex_bundle = fetch_sec_earnings_exhibits(cik, acc, config["sec_user_agent"], form_type=form)
    ex_text = ex_bundle.get("text", "")
    if not ex_text:
        print(f"  [스킵] {ticker} ({acc}) — Exhibit 본문 추출 실패")
        return False

    if form.startswith("6-K") and not is_6k_earnings_document(ex_text):
        print(f"  [스킵] {ticker} ({acc}) — 실적 발표가 아닌 일반 6-K 공시")
        return False

    # 사전 스냅샷 및 최신 Yahoo 번들 로드
    all_snaps = load_json_file(SNAPSHOT_PATH, {})
    day_snap = all_snaps.get(f_date)
    yahoo_bundle = fetch_yahoo_consensus_bundle(ticker)

    # Method B: Gemini 구조화 추출 -> API 키 미설정/실패 시 결정론적 폴백
    extraction = extract_earnings_with_gemini(ticker, f_date, ex_text, config)
    parser_mode = "Gemini"
    if extraction is None:
        extraction = extract_earnings_fallback(ex_text, yahoo_bundle=yahoo_bundle)
        parser_mode = "Fallback"

    if not extraction.is_earnings_release:
        print(f"  [스킵] {ticker} ({acc}) — 실적 발표 본문 아님")
        return False

    supplemental = fetch_grounded_supplemental_consensus(ticker, extraction, config)
    cons = resolve_consensus_for_filing(
        ticker=ticker,
        extraction=extraction,
        filing_date_str=f_date,
        day_snap=day_snap,
        yahoo_bundle=yahoo_bundle,
        supplemental=supplemental,
    )

    # 요구사항 #3: 컨센서스가 있는 종목만 발송
    if not cons.get("has_consensus"):
        print(f"  [컨센서스 부재 스킵] {ticker} ({acc}) — 월가 컨센서스 없음")
        return False

    kr_name = get_korean_stock_name(ticker, universe_names)
    msg = build_earnings_message(ticker, kr_name, extraction, cons)
    if not msg:
        print(f"  [스킵] {ticker} ({acc}) — 컨센서스 대조 불가")
        return False

    print(f"[실적 포착 | {parser_mode}] {ticker}({kr_name}) | 공시={acc} ({f_date}) | 출처={ex_bundle.get('url')}")
    sent = send_telegram_message(msg, config, dry_run=dry_run)
    if sent and not dry_run and seen_cache is not None:
        seen_cache.add(acc)
        seen_cache.add(f"SENT:{f_date}:{ticker}")
        save_json_file(SEEN_CACHE_PATH, sorted(seen_cache))
    return sent


# =====================================================================
# 9. 메인 데몬 루프 & CLI 실행 진입점
# =====================================================================
def _current_poll_interval(now_et: datetime.datetime, config: Dict[str, Any]) -> int:
    if now_et.weekday() >= 5:
        return int(config.get("idle_poll_interval_sec", 300))
    t = now_et.time()
    # BMO(06:00~09:30 ET) & AMC(16:00~19:30 ET) 집중 폴링
    if (datetime.time(6, 0) <= t <= datetime.time(9, 30)) or (datetime.time(16, 0) <= t <= datetime.time(19, 30)):
        return int(config.get("active_poll_interval_sec", 45))
    # 정규장(09:30~16:00 ET) 저빈도 폴링
    if datetime.time(9, 30) < t < datetime.time(16, 0):
        return int(config.get("regular_poll_interval_sec", 240))
    return int(config.get("idle_poll_interval_sec", 300))


def run_cycle(
    universe_set: set,
    universe_names: Dict[str, str],
    cik_to_ticker: Dict[str, str],
    seen_cache: set,
    config: Dict[str, Any],
    dry_run: bool = False,
) -> None:
    now_et = datetime.datetime.now(ET)
    et_date_str = now_et.strftime("%Y-%m-%d")

    if now_et.weekday() < 5 and now_et.time() >= datetime.time(5, 30):
        day_snap = build_daily_consensus_snapshot(et_date_str, universe_set, universe_names)
        brief_key = f"DAILY_BRIEF:{et_date_str}"
        if now_et.time() >= datetime.time(6, 0) and brief_key not in seen_cache:
            sched_msg = build_major_schedule_message(et_date_str, day_snap, universe_names, config)
            if sched_msg:
                if send_telegram_message(sched_msg, config, dry_run=dry_run) and not dry_run:
                    seen_cache.add(brief_key)
                    save_json_file(SEEN_CACHE_PATH, sorted(seen_cache))

    filings = poll_sec_filings(cik_to_ticker, seen_cache, et_date_str, config["sec_user_agent"])
    if filings:
        print(f"[{now_et.strftime('%H:%M:%S')} ET] 신규 SEC 실적 공시 후보 {len(filings)}건 감지")
    for idx, f_info in enumerate(filings[:MAX_FILINGS_PER_RUN]):
        ok = process_single_filing(f_info, universe_names, config, seen_cache=seen_cache, dry_run=dry_run)
        if not ok and not dry_run:
            # 재시도 무한루프 방지를 위해 스킵된 공시 번호 캐시
            seen_cache.add(f_info["accession"])
            save_json_file(SEEN_CACHE_PATH, sorted(seen_cache))
        if idx + 1 < min(len(filings), MAX_FILINGS_PER_RUN):
            time.sleep(1.5)


def bootstrap_seen_filings(config: Dict[str, Any]) -> None:
    """
    초기 배포 직후 과거/당일 기발표 공시 폭주를 막기 위해 현재 SEC 8-K/6-K 공시와
    당일 일정 브리핑 키를 텔레그램 발송 없이 seen_us_earnings.json에 초기 등록합니다.
    """
    _, universe_set, _ = load_v12_universe()
    cik_to_ticker, _ = load_sec_cik_map(universe_set, config["sec_user_agent"])
    seen_list = load_json_file(SEEN_CACHE_PATH, [])
    seen_cache = set(seen_list) if isinstance(seen_list, list) else set()

    now_et = datetime.datetime.now(ET)
    et_date_str = now_et.strftime("%Y-%m-%d")
    filings = poll_sec_filings(cik_to_ticker, seen_cache, et_date_str, config["sec_user_agent"])
    for f_info in filings:
        seen_cache.add(f_info["accession"])
    seen_cache.add(f"DAILY_BRIEF:{et_date_str}")
    save_json_file(SEEN_CACHE_PATH, sorted(seen_cache))
    print(f"[Bootstrap 완료] 신규 등록 {len(filings)}건 / 총 캐시 {len(seen_cache):,}건 ({SEEN_CACHE_PATH})")


def run_realtime_daemon(config: Dict[str, Any], dry_run: bool = False, once: bool = False) -> None:
    print("=" * 68)
    print("US Stock Earnings Intelligence Bot v2 (Method B: SEC 8-K + Gemini)")
    print(f"• 발송 채널: {config.get('telegram_chat_id')} | 모드: {'DRY-RUN' if dry_run else 'LIVE'}")
    print(f"• Gemini 모델: {config.get('gemini_model')} (API Key: {'설정됨' if config.get('gemini_api_key') else '미설정 - 폴백 파서 활성'})")
    print("=" * 68)

    _, universe_set, universe_names = load_v12_universe()
    cik_to_ticker, _ = load_sec_cik_map(universe_set, config["sec_user_agent"])
    seen_list = load_json_file(SEEN_CACHE_PATH, [])
    seen_cache = set(seen_list) if isinstance(seen_list, list) else set()
    print(f"[초기화 완료] 기처리 캐시 {len(seen_cache):,}건 로드")

    while True:
        try:
            run_cycle(universe_set, universe_names, cik_to_ticker, seen_cache, config, dry_run=dry_run)
        except KeyboardInterrupt:
            print("\n[종료] 사용자 요청으로 봇을 종료합니다.")
            break
        except Exception as e:
            print(f"[예외] 사이클 실행 오류 (30초 후 재시도): {e}")
            if once:
                break
            time.sleep(30)
            continue

        if once:
            break
        interval = _current_poll_interval(datetime.datetime.now(ET), config)
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description="US Stock Earnings Tracker Bot v2")
    parser.add_argument("--dry-run", "--no-telegram", dest="dry_run", action="store_true", help="텔레그램 미전송 콘솔 검증 모드")
    parser.add_argument("--test-ticker", type=str, help="특정 미국 티커의 최신 SEC 실적 공시(8-K/6-K)로 전체 파이프라인 테스트")
    parser.add_argument("--preview-schedule", nargs="?", const="TODAY", help="특정 날짜(YYYY-MM-DD) 주요 실적 일정 브리핑 미리보기")
    parser.add_argument("--bootstrap-seen", action="store_true", help="텔레그램 발송 없이 현재 SEC 공시를 기처리 캐시에 초기 등록")
    parser.add_argument("--once", action="store_true", help="1회 스캔 사이클만 실행 후 종료")
    args = parser.parse_args()

    is_test_cmd = bool(args.dry_run or args.test_ticker or args.preview_schedule or args.bootstrap_seen)
    if not is_test_cmd:
        if not acquire_single_instance_lock("v2_earnings_daemon.lock"):
            print("이미 실행 중인 US Earnings Bot v2 프로세스가 있습니다.")
            sys.exit(0)

    config = load_config()

    if args.bootstrap_seen:
        bootstrap_seen_filings(config)
        return

    if args.preview_schedule:
        target_date = datetime.datetime.now(ET).strftime("%Y-%m-%d") if args.preview_schedule == "TODAY" else args.preview_schedule
        _, universe_set, universe_names = load_v12_universe()
        day_snap = build_daily_consensus_snapshot(target_date, universe_set, universe_names, force=True)
        sched_msg = build_major_schedule_message(target_date, day_snap, universe_names, config)
        if sched_msg:
            send_telegram_message(sched_msg, config, dry_run=True if args.dry_run else False)
        else:
            print(f"[안내] {target_date} 기준 컨센서스를 보유한 주요 실적발표 예정 종목이 없습니다.")
        return

    if args.test_ticker:
        sym = normalize_ticker(args.test_ticker)
        _, universe_set, universe_names = load_v12_universe()
        _, ticker_to_cik = load_sec_cik_map(universe_set, config["sec_user_agent"])
        cik = ticker_to_cik.get(sym)
        if not cik:
            print(f"[오류] v12 유니버스 또는 SEC CIK 매핑에서 {sym}을 찾을 수 없습니다.")
            sys.exit(1)
        latest = find_latest_sec_earnings_filing(cik, config["sec_user_agent"])
        if not latest:
            print(f"[오류] {sym}의 최근 SEC 8-K(2.02) / 6-K 공시를 찾지 못했습니다.")
            sys.exit(1)
        latest["ticker"] = sym
        process_single_filing(latest, universe_names, config, seen_cache=None, dry_run=True if args.dry_run else False)
        return

    run_realtime_daemon(config, dry_run=args.dry_run, once=args.once)


if __name__ == "__main__":
    main()
