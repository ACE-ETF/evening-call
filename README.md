# Korea Market Radar

일간·주간 한국 증시 & 섹터 레이더 대시보드.
평일 KST 16:05(+17:00 보완) GitHub Actions가 `fetch_market.py`를 실행해 `data.json`을 갱신하고, GitHub Pages가 `index.html`을 배포합니다.

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
