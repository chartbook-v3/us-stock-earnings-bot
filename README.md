# US Stock Earnings Intelligence Bot v2 (24/7 GitHub Actions)

S&P 500 · NASDAQ 100 · Russell 3000 전종목(약 2,589개)을 대상으로 미국 기업 실적 공시(`SEC EDGAR 8-K Item 2.02` / `6-K`)를 24/7 무중단 추적하고, **Gemini API (`gemini-4-hydrogen`)**로 실적·가이던스를 구조화 추출하여 월가 컨센서스와 대조한 뒤 텔레그램(`@uswatch3`)으로 실시간 브리핑하는 서버리스 봇입니다.

## 주요 기능
- **장전(ET 05:30+) 스냅샷 & 일정 브리핑**: Nasdaq 실적 캘린더 + Yahoo Finance 컨센서스(`0q`, `+1q`, `0y`, `+1y`)를 사전 박제하고 주요 발표 일정을 매일 1회 요약 전송
- **실시간 SEC EDGAR 감시**: Atom 피드 + EFTS 백스톱으로 `8-K Item 2.02` 및 `6-K` Exhibit 99.1/99.2 본문 자동 수집
- **Method B 구조화 파싱**: Non-GAAP 매출·EPS·GPM·OPM 및 다음분기/다음 회계연도 가이던스 밴드 자동 추출 (API 장애 시 정규식 폴백 지원)
- **무중단 서버리스 실행**: GitHub Actions 크론으로 평일 BMO/AMC 집중 시간대 5분 주기·정규장 15분 주기 자동 실행 (`state/` Git 백업으로 중복 발송 영구 방지)

## 필수 GitHub Secrets 설정 (`Settings -> Secrets and variables -> Actions`)
- `GEMINI_API_KEY`: Google AI Studio Gemini API 키
- `TELEGRAM_BOT_TOKEN`: 텔레그램 봇 토큰
- `TELEGRAM_CHAT_ID`: 발송 대상 채널 ID (`@uswatch3`)
