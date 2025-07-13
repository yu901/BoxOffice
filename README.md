# BoxOffice Data-Pipeline & Dashboard

이 프로젝트는 대한민국 영화 시장 데이터를 수집, 처리하고 시각화하는 데이터 파이프라인 및 대시보드입니다. 영화진흥위원회(KOBIS)의 Open API를 통해 일일 박스오피스 데이터를 수집하고, CGV, 롯데시네마, 메가박스 웹사이트에서 영화 굿즈 이벤트 및 재고 정보를 스크레이핑합니다. 수집된 데이터는 Dagster 파이프라인을 통해 처리되며, Streamlit으로 구축된 대시보드에서 상호작용하며 탐색할 수 있습니다.

## ✨ 주요 기능

- **자동화된 데이터 수집**: Dagster 스케줄러를 통해 매일 자동으로 박스오피스 데이터와 굿즈 이벤트/재고 정보를 수집합니다.
- **멀티 소스 스크레이핑**: CGV, 롯데시네마, 메가박스의 이벤트 및 재고 현황을 안정적으로 스크레이핑합니다.
- **데이터 파이프라인 관리**: Dagster를 사용하여 데이터 수집 및 저장 파이프라인을 체계적으로 관리하고 모니터링합니다.
- **데이터 영속성**: 수집된 모든 데이터는 로컬 SQLite 데이터베이스에 저장됩니다.
- **인터랙티브 대시보드**: Streamlit 기반의 대시보드를 통해 다음 정보를 시각적으로 탐색할 수 있습니다.
  - **AI 데이터 분석가**: Gemini API를 활용한 AI 챗봇을 통해 자연어로 영화 데이터를 질문하고 분석 결과를 얻을 수 있습니다.
  - **박스오피스 분석**: 일별 및 기간별 박스오피스 순위와 흥행 추이를 시각적으로 분석합니다.
  - **굿즈 재고 현황**: 현재 진행 중인 영화 굿즈 이벤트 목록과 각 이벤트별 굿즈의 실시간 지점별 재고를 확인합니다.
- **편리한 실행 스크립트**: 셸 스크립트를 통해 애플리케이션 서비스를 손쉽게 시작하고 중지할 수 있습니다.

## 📂 프로젝트 구조

```
BoxOffice/
├── .pids/              # (자동 생성) 서비스 PID 파일 저장소
├── db/                 # SQLite 데이터베이스 파일 저장소
├── scripts/            # 서비스 실행/중지 셸 스크립트
│   ├── run_dagster.sh
│   ├── run_dashboard.sh
│   ├── stop_dagster.sh
│   └── stop_dashboard.sh
├── src/
│   ├── boxoffice/
│   │   ├── logic/      # 핵심 로직 (스크레이퍼, API 추출기, DB 커넥터)
│   │   └── pipelines/  # Dagster 파이프라인 정의
│   ├── scripts/        # Python 유틸리티 스크립트 (데이터 백필 등)
│   ├── test/           # 테스트 스크립트
│   ├── dashboard.py    # Streamlit 대시보드 애플리케이션
│   └── definitions.py  # Dagster Job/Schedule 정의
├── .gitignore
├── README.md           # 프로젝트 안내 파일
└── requirements.txt    # Python 의존성 목록
```

## 🚀 설치 및 설정

1.  **리포지토리 복제**
    ```bash
    git clone <repository-url>
    cd BoxOffice
    ```

2.  **가상환경 생성 및 활성화**
    ```bash
    python -m venv venv
    source venv/bin/activate
    ```

3.  **의존성 설치**
    ```bash
    pip install -r requirements.txt
    ```

4.  **API 키 설정**
    프로젝트의 모든 기능을 사용하려면 아래 두 가지 API 키가 필요합니다.
    -   `config/config.yml` 파일을 엽니다.
    -   **KOBIS API 키**: 영화진흥위원회(KOBIS) Open API에서 키를 발급받아 `kobis:` 섹션의 `key:` 값을 교체합니다.
    -   **Gemini API 키**: Google AI Studio에서 키를 발급받아 `gemini:` 섹션의 `api_key:` 값을 교체합니다.

## 🏃‍♀️ 실행 방법

먼저, 셸 스크립트에 실행 권한을 부여합니다.

```bash
chmod +x scripts/*.sh
```

### 서비스 시작 (백그라운드 실행)

- **Dagster UI & Daemon 시작**
  ```bash
  ./scripts/run_dagster.sh
  ```
- **Streamlit 대시보드 시작**
  ```bash
  ./scripts/run_dashboard.sh
  ```

### 서비스 접속

- **Dagster UI**: 웹 브라우저에서 `http://localhost:3000` 주소로 접속하세요.
- **Streamlit 대시보드**: 웹 브라우저에서 `http://localhost:8501` 주소로 접속하여 'AI 데이터 분석가' 탭을 포함한 다양한 기능을 사용해 보세요.

### 서비스 중지

- **Dagster 중지**
  ```bash
  ./scripts/stop_dagster.sh
  ```
- **Streamlit 대시보드 중지**
  ```bash
  ./scripts/stop_dashboard.sh
  ```

### 데이터 백필 (선택 사항)

데이터베이스를 과거 데이터로 채우고 싶을 때 아래 스크립트를 실행하세요.

- **박스오피스 데이터 백필** (스크립트 내 날짜 수정 가능)
  ```bash
  python src/scripts/backfill_boxoffice.py
  ```
- **굿즈 이벤트 데이터 백필**
  ```bash
  python src/scripts/backfill_goods_events.py
  ```

## 🛠️ 기술 스택

- **Backend & Data Pipeline**: Python, Dagster
- **AI & LLM**: Google Gemini API
- **Dashboard**: Streamlit, Altair
- **Data Handling**: Pandas, SQLAlchemy
- **Database**: SQLite
- **Web Scraping**: Requests, BeautifulSoup

---

*이 README는 Gemini Code Assist에 의해 작성되었습니다.*