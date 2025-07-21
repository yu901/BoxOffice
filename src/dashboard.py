import streamlit as st
import pandas as pd
from boxoffice.logic.sqlite_connector import SQLiteConnector
import altair as alt
from boxoffice.logic.ai_agent import AIAgent

st.set_page_config(layout="wide")

@st.cache_data(ttl=600) # Cache data for 10 minutes
def load_data():
    """Loads data from the SQLite database."""
    db = SQLiteConnector()
    boxoffice_df = db.select_query("SELECT * FROM boxoffice ORDER BY targetDt DESC, rank ASC")
    stock_df = db.select_query("SELECT * FROM goods_stock")
    event_df = db.select_query("SELECT * FROM goods_event ORDER BY start_date DESC")
    return boxoffice_df, stock_df, event_df

def show_boxoffice_dashboard(df):
    """Displays the daily box office dashboard."""
    st.title("📊 일일 박스오피스 현황")

    if df.empty:
        st.warning("박스오피스 데이터가 없습니다.")
        return

    df['targetDt_date'] = pd.to_datetime(df['targetDt']).dt.date
    
    # Date selector
    available_dates = df['targetDt_date'].unique()
    min_date = available_dates.min()
    max_date = available_dates.max()
    
    selected_date = st.date_input(
        "날짜 선택",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
    )

    st.header(f"🗓️ {selected_date.strftime('%Y-%m-%d')} 기준")

    display_df = df[df['targetDt_date'] == selected_date]

    # Top 3 movies
    cols = st.columns(3)
    # iterrows()의 인덱스는 원본 DataFrame의 인덱스를 유지하므로 0, 1, 2가 아닐 수 있습니다.
    # 열(column)에 안전하게 접근하기 위해 enumerate를 사용하여 0부터 시작하는 인덱스를 얻습니다.
    for i, (idx, movie) in enumerate(display_df.head(3).iterrows()):
        with cols[i]: # 열 인덱스로 i를 사용합니다.
            st.metric(
                label=f"🥇 {movie['rank']}위: {movie['movieNm']}",
                value=f"{int(movie['audiCnt']):,} 명",
                delta=f"{int(movie['audiInten']):,} 명"
            )

    st.subheader("📈 일일 관객수 Top 10")
    
    top_10_df = display_df.head(10)

    # 일일 관객수 막대그래프
    bar_chart = alt.Chart(top_10_df).mark_bar().encode(
        x=alt.X('movieNm:N', sort=alt.EncodingSortField(field="audiCnt", op="sum", order='descending'), title="영화 제목"),
        y=alt.Y('audiCnt:Q', title="관객수", axis=alt.Axis(format='~s')),
        tooltip=[alt.Tooltip('movieNm', title='영화명'), alt.Tooltip('audiCnt', title='일일 관객수', format=',')]
    )

    # 누적 관객수 라인그래프
    line_chart = alt.Chart(top_10_df).mark_line(color='red', point=True).encode(
        x=alt.X('movieNm:N', sort=alt.EncodingSortField(field="audiCnt", op="sum", order='descending')),
        y=alt.Y('audiAcc:Q', title="누적 관객수", axis=alt.Axis(format='~s')),
        tooltip=[alt.Tooltip('movieNm', title='영화명'), alt.Tooltip('audiAcc', title='누적 관객수', format=',')]
    )

    # 두 차트 결합 (오른쪽 Y축 사용)
    combined_chart = alt.layer(bar_chart, line_chart).resolve_scale(
        y='independent'
    )

    st.altair_chart(combined_chart, use_container_width=True)

    st.subheader("📋 상세 데이터")
    
    # 보여줄 컬럼과 한글 이름 매핑
    display_columns = {
        "rank": "순위",
        "movieNm": "영화명",
        "audiCnt": "일일 관객수",
        "audiAcc": "누적 관객수",
        "salesAmt": "일일 매출액",
        "openDt": "개봉일",
    }
    
    display_df_formatted = display_df[list(display_columns.keys())].rename(columns=display_columns)
    
    # 날짜 형식 변경
    display_df_formatted['개봉일'] = pd.to_datetime(display_df_formatted['개봉일']).dt.strftime('%Y-%m-%d')

    st.dataframe(display_df_formatted.style.format({
        "일일 관객수": "{:,.0f}",
        "누적 관객수": "{:,.0f}",
        "일일 매출액": "{:,.0f}",
    }), hide_index=True)

def show_overall_boxoffice_dashboard(df):
    """Displays the overall box office analysis dashboard."""
    st.title("📈 박스오피스 개요")

    if df.empty:
        st.warning("박스오피스 데이터가 없습니다.")
        return

    df['targetDt_date'] = pd.to_datetime(df['targetDt']).dt.date
    min_db_date = df['targetDt_date'].min()
    max_db_date = df['targetDt_date'].max()

    # 1. Date Range Selector
    st.header("기간 선택")
    cols = st.columns(2)
    with cols[0]:
        start_date = st.date_input("시작일", value=max_db_date - pd.Timedelta(days=30), min_value=min_db_date, max_value=max_db_date)
    with cols[1]:
        end_date = st.date_input("종료일", value=max_db_date, min_value=min_db_date, max_value=max_db_date)

    if start_date > end_date:
        st.error("시작일은 종료일보다 이전이어야 합니다.")
        return

    # Filter data based on selected date range
    filtered_df = df[(df['targetDt_date'] >= start_date) & (df['targetDt_date'] <= end_date)]

    # 2. Overall Trends
    st.header("기간별 박스오피스 추이")
    daily_total_audience = filtered_df.groupby('targetDt_date')['audiCnt'].sum().reset_index()
    trend_chart = alt.Chart(daily_total_audience).mark_line().encode(
        x=alt.X('targetDt_date:T', title='날짜'),
        y=alt.Y('audiCnt:Q', title='총 관객수'),
        tooltip=['targetDt_date', 'audiCnt']
    ).interactive()
    st.altair_chart(trend_chart, use_container_width=True)

    # 3. Top Performing Movies
    st.header(f"기간별 흥행 영화 TOP 10 ({start_date} ~ {end_date})")
    top_movies_by_audience = filtered_df.groupby('movieNm')['audiCnt'].sum().nlargest(10).reset_index()
    
    top_movies_chart = alt.Chart(top_movies_by_audience).mark_bar().encode(
        x=alt.X('movieNm:N', sort='-y', title='영화 제목'),
        y=alt.Y('audiCnt:Q', title='총 관객수'),
        tooltip=['movieNm', 'audiCnt']
    )
    st.altair_chart(top_movies_chart, use_container_width=True)

    # 4. Detailed Movie Performance
    st.header("주요 영화별 흥행 추이")
    top_movie_names = top_movies_by_audience['movieNm'].tolist()
    selected_movies = st.multiselect("비교할 영화를 선택하세요:", options=top_movie_names, default=top_movie_names[:3])

    if selected_movies:
        movie_trend_df = filtered_df[filtered_df['movieNm'].isin(selected_movies)]
        movie_trend_chart = alt.Chart(movie_trend_df).mark_line().encode(
            x=alt.X('targetDt_date:T', title='날짜'),
            y=alt.Y('audiCnt:Q', title='일일 관객수'),
            color='movieNm:N',
            tooltip=['targetDt_date', 'movieNm', 'audiCnt']
        ).interactive()
        st.altair_chart(movie_trend_chart, use_container_width=True)

def show_goods_stock_dashboard(stock_df, events_df):
    """Displays the goods stock dashboard."""
    st.title("🎁 영화 굿즈 재고 현황")

    if events_df.empty:
        st.info("현재 진행중인 굿즈 이벤트가 없습니다.")
        return

    # Display latest stock update time if stock data exists
    if not stock_df.empty:
        # DB에서 읽어온 scraped_at은 문자열일 수 있으므로 datetime으로 변환
        stock_df['scraped_at'] = pd.to_datetime(stock_df['scraped_at'])
        latest_scrape_time = stock_df['scraped_at'].max()
        st.header(f"⏰ 마지막 업데이트: {latest_scrape_time.strftime('%Y-%m-%d %H:%M:%S')}")
        # 가장 최근 재고 데이터만 사용
        latest_stock_df = stock_df[stock_df['scraped_at'] == latest_scrape_time].copy()
    else:
        st.warning("수집된 재고 데이터가 없습니다. 이벤트 목록만 표시됩니다.")
        latest_stock_df = pd.DataFrame()

    # 원본 이벤트 데이터프레임은 필터링을 위해 유지
    events_df_original = events_df.copy()

    if events_df.empty:
        st.info("현재 진행중인 굿즈 이벤트가 없습니다.")
        return

    # 종료일이 지난 이벤트는 필터링합니다.
    # errors='coerce'는 잘못된 날짜 형식을 NaT (Not a Time)으로 변환하여 오류를 방지합니다.
    events_df['end_date_dt'] = pd.to_datetime(events_df['end_date'], errors='coerce')
    events_df = events_df[events_df['end_date_dt'] >= pd.Timestamp.now().normalize()]

    # --- 필터링 UI ---
    st.subheader("🔎 이벤트 필터")
    filter_cols = st.columns(2)
    
    # 1. 영화관 필터
    theater_options = ["전체"] + events_df['theater_chain'].unique().tolist()
    selected_theater = filter_cols[0].radio("영화관 선택", options=theater_options, horizontal=True)
    
    if selected_theater != "전체":
        events_df = events_df[events_df['theater_chain'] == selected_theater]

    # 2. 영화 필터
    movie_options = ["전체"] + sorted(events_df['movie_title'].dropna().unique().tolist())
    selected_movie = filter_cols[1].selectbox("영화 선택", options=movie_options)

    if selected_movie != "전체":
        events_df = events_df[events_df['movie_title'] == selected_movie]


    st.subheader("🎟️ 현재 진행중인 굿즈 이벤트")

    # --- Single-select checkbox logic ---
    # 1. 세션 상태를 사용하여 단일 선택된 이벤트의 ID를 기억합니다.
    if 'selected_event_id' not in st.session_state:
        st.session_state.selected_event_id = None

    # 2. 표시할 데이터프레임을 만들고, 세션 상태에 따라 체크박스 값을 설정합니다.
    required_cols = [
        "theater_chain", "movie_title", "goods_name", "start_date", "end_date", "event_url",
        "event_id", "event_title", "image_url"
    ]
    events_df_display = events_df[required_cols].copy().reset_index(drop=True)
    events_df_display['재고 현황 보기'] = (events_df_display['event_id'] == st.session_state.selected_event_id)
    events_df_display.insert(0, "재고 현황 보기", events_df_display.pop('재고 현황 보기'))

    # 3. data_editor를 렌더링합니다.
    edited_df = st.data_editor(
        events_df_display,
        column_config={
            "재고 현황 보기": st.column_config.CheckboxColumn("재고 보기", required=True),
            "theater_chain": "영화관",
            "movie_title": "영화 제목",
            "goods_name": "굿즈명",
            "start_date": "시작일",
            "end_date": "종료일",
            "event_url": st.column_config.LinkColumn("이벤트 페이지", display_text="링크"),
            # 사용자에게 보여줄 필요 없는 컬럼은 숨깁니다.
            "event_id": None,
            "event_title": None,
            "goods_id": None,
            "image_url": None,
            "end_date_dt": None, # 내부 계산용 컬럼 숨기기
        },
        disabled=["theater_chain", "movie_title", "goods_name", "start_date", "end_date", "event_url"],
        hide_index=True,
        column_order=("재고 현황 보기", "theater_chain", "movie_title", "goods_name", "start_date", "end_date", "event_url")
    )

    # 4. 사용자 입력을 처리하고, 선택이 변경되면 세션 상태를 업데이트하고 UI를 새로고침합니다.
    # 사용자가 data_editor와 상호작용한 후의 체크된 행들을 가져옵니다.
    checked_rows_after_edit = edited_df[edited_df["재고 현황 보기"]]
    
    # 새로 선택된 ID를 결정합니다. (행 순서에 의존하지 않는 방식으로 개선)
    newly_selected_id = None
    checked_ids_after_edit = set(checked_rows_after_edit['event_id'])
    current_id = st.session_state.selected_event_id

    if not checked_ids_after_edit:
        # 모든 체크박스가 해제된 경우
        newly_selected_id = None
    elif len(checked_ids_after_edit) == 1:
        # 하나만 체크된 경우 (초기 선택 또는 이전 선택 해제 후 새 선택)
        newly_selected_id = checked_ids_after_edit.pop()
    else:  # 여러 개가 체크된 경우 (기존 선택 + 새로운 선택)
        # 기존에 선택된 ID를 제외한 나머지(새로 선택된 ID)를 찾습니다.
        new_ids = checked_ids_after_edit - {current_id}
        if new_ids:
            newly_selected_id = new_ids.pop()

    if st.session_state.selected_event_id != newly_selected_id:
        st.session_state.selected_event_id = newly_selected_id
        st.rerun()

    # --- End of logic ---

    checked_rows = edited_df[edited_df["재고 현황 보기"]]

    # Define expander title and expansion state
    if not checked_rows.empty:
        selected_event_title = checked_rows.iloc[-1]['event_title']
        expander_title = f"**'{selected_event_title}'** 지점별 재고 현황"
    else:
        expander_title = "지점별 재고 현황"

    # CSS for the bordered and centered image container
    custom_css = """
    <style>
        /* Force the container holding st.columns to be a flex container */
        div[data-testid="stExpander"] > div > div[data-testid="stVerticalBlock"] > div[data-testid="stHorizontalBlock"] {
            display: flex;
            align-items: stretch; /* Make children stretch to fill height */
        }

        /* Apply border and centering to the image container */
        .custom-image-wrapper {
            border: 1px solid #ccc; /* Light gray border */
            border-radius: 5px; /* Slightly rounded corners */
            padding: 10px; /* Padding inside the border */
            display: flex;
            flex-direction: column; /* Allow content to stack if needed */
            justify-content: center; /* Center vertically */
            align-items: center; /* Center horizontally */
            height: 100%; /* Make it take full height of its parent column */
            min-height: 400px; /* Minimum height to match dataframe, adjust as needed */
            max-height: 400px; /* 이미지 높이 제한 추가 */
            overflow: hidden; /* Hide overflow if image is too big */
        }
        .custom-image-wrapper img {
            width: 100%; /* Ensure image fills the width of its wrapper */
            height: auto; /* Maintain aspect ratio */
            max-height: 100%; /* But don't overflow vertically */
            object-fit: contain; /* Still contain within the box */
        }
    </style>
    """
    st.markdown(custom_css, unsafe_allow_html=True)

    with st.expander(expander_title, expanded=True):
        if not checked_rows.empty:
            if not latest_stock_df.empty:
                selected_row = checked_rows.iloc[-1]
                selected_event_id = selected_row['event_id']
                selected_image_url = selected_row.get('image_url') # Get image_url

                # Display image and stock data side-by-side
                col_img, col_table = st.columns([2, 3]) # Adjust column width ratio as needed

                with col_img:
                    if selected_image_url:
                        st.markdown(f"""
                            <div class="custom-image-wrapper">
                                <img src="{selected_image_url}" alt="Event Image">
                            </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown("<div class=\"custom-image-wrapper\">이미지 없음</div>", unsafe_allow_html=True)

                with col_table:
                    stock_display_df = latest_stock_df[
                        latest_stock_df['event_id'] == selected_event_id
                    ]
                    stock_display_df = stock_display_df.sort_values(by='theater_name')
                    stock_display_cols = {"theater_name": "지점명", "status": "재고 상태"}
                    
                    # 재고 상태에 따라 색상을 적용하는 함수 (Series 전체에 적용)
                    def color_status(s: pd.Series) -> list[str]:
                        color_map = {
                            "보유": "#2ECC71",      # 초록색
                            "소진중": "#F39C12",    # 주황색
                            "소량보유": "#E74C3C",  # 빨강색
                            "소진": "#95A5A6"       # 회색
                        }
                        return [f'color: {color_map.get(v, "black")}' for v in s]

                    display_data = stock_display_df[list(stock_display_cols.keys())].rename(columns=stock_display_cols)
                    st.dataframe(display_data.style.apply(color_status, subset=['재고 상태']), hide_index=True, height=400, use_container_width=True)
            else:
                st.warning("재고 현황을 보려면 재고 수집 작업이 실행되어야 합니다.")
        else:
            st.info('지점별 재고 현황을 확인하려면 현재 진행중인 굿즈 이벤트의 재고 보기를 클릭하세요.')

@st.cache_resource
def get_ai_agent():
    """AI 에이전트를 초기화하고 캐시합니다."""
    try:
        return AIAgent()
    except ValueError as e:
        st.error(e)
        return None

def show_ai_chat_dashboard():
    """AI 챗봇 대시보드를 표시합니다."""
    st.title("🤖 AI 영화 데이터 분석가")
    st.info("영화 데이터에 대해 궁금한 점을 자유롭게 물어보세요! (예: '최근 7일간 가장 많은 관객을 동원한 영화 3개 알려줘', '범죄도시4의 누적 관객수는?')")

    agent = get_ai_agent()
    if not agent:
        return

    # 세션 상태에 채팅 기록 초기화
    if "ai_messages" not in st.session_state:
        st.session_state.ai_messages = []

    # 이전 채팅 기록 표시
    for message in st.session_state.ai_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if "sql" in message and message["sql"]:
                with st.expander("실행된 SQL 쿼리 보기"):
                    st.code(message["sql"], language="sql")

    # 사용자 입력 처리
    if prompt := st.chat_input("질문을 입력하세요..."):
        # 사용자 메시지 표시 및 기록
        st.chat_message("user").markdown(prompt)
        st.session_state.ai_messages.append({"role": "user", "content": prompt})

        # AI 응답 생성 및 표시
        with st.chat_message("assistant"):
            with st.spinner("답변을 생각하는 중..."):
                response = agent.ask(prompt)
                answer = response["answer"]
                sql_query = response["sql_query"]
                
                st.markdown(answer)
                with st.expander("실행된 SQL 쿼리 보기"):
                    st.code(sql_query, language="sql")

        # AI 응답 기록
        st.session_state.ai_messages.append({"role": "assistant", "content": answer, "sql": sql_query})

def main():
    boxoffice_df, stock_df, event_df = load_data()

    st.sidebar.title("대시보드 선택")
    page = st.sidebar.radio("이동", ["AI 데이터 분석가", "박스오피스 개요", "일일 박스오피스", "굿즈 재고 현황"])

    if page == "AI 데이터 분석가":
        show_ai_chat_dashboard()
    elif page == "박스오피스 개요":
        show_overall_boxoffice_dashboard(boxoffice_df)
    elif page == "일일 박스오피스":
        show_boxoffice_dashboard(boxoffice_df)
    elif page == "굿즈 재고 현황":
        show_goods_stock_dashboard(stock_df, event_df)

if __name__ == "__main__":
    main()