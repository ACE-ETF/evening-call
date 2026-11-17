# Korea Market Radar

일간·주간 한국 증시 & 섹터 레이더 대시보드.
평일 KST 16:30(1차) → 17:15(마감 기사 보강) → 18:05(수급 확정) GitHub Actions가 `fetch_market.py`를 실행해 `data.json`을 갱신하고, GitHub Pages가 `index.html`을 배포합니다.

- 대시보드: https://ace-etf.github.io/korea-market-radar/
- 수동 실행: Actions → Auto Update Korea Market Radar → Run workflow
- Gemini 브리핑: Settings → Secrets and variables → Actions 에 `GEMINI_API_KEY` 등록 (없으면 규칙 기반 문장)

```text
korea-market-radar/
├── .github/workflows/update.yml
├── fetch_market.py
├── data.json        # 자동 생성
└── index.html
```

## 누적 데이터
- `history/YYYY-MM-DD.json`: 거래일별 최종본(대시보드 전체 데이터)
- `history/index.json`: 날짜별 요약 목록(코스피·코스닥 종가/등락률, 투자자별 수급, 브리핑 헤드라인)
