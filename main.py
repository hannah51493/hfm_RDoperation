import streamlit as st
import pandas as pd
import re
from io import BytesIO
from custom_functions import custom_functions

# =====================
# 페이지 설정
# =====================
st.set_page_config(page_title="ADEF HFM Report", layout="wide")
st.title("📊 ADEF HFM Report 생성기")

# =====================
# 유틸 함수
# =====================
def load_excel_sheets(file):
    """엑셀 파일의 모든 시트를 딕셔너리로 반환"""
    return pd.read_excel(file, sheet_name=None)


def get_config_row(config_file_df, media_name):
    """Config_File 시트에서 매체 설정 행 반환"""
    row = config_file_df[config_file_df['매체명'] == media_name]
    if row.empty:
        raise ValueError(f"Config_File 에서 '{media_name}' 매체를 찾을 수 없습니다.")
    return row.iloc[0]


def read_media_file(file, config_row):
    """매체 파일을 Config_File 설정 기준으로 읽기"""
    header_row  = int(config_row['헤더행']) - 1
    file_format = config_row['파일형식'].lower()

    if file_format == 'csv':
        df = pd.read_csv(file, header=header_row)
    else:
        df = pd.read_excel(file, header=header_row)

    return df


def apply_config_column(df, config_col_df, media_name, config_sheets, year_month=None):
    """
    Config_Column 시트 기준으로 가공 로직 순서대로 적용
    """
    # 열 이름 공백 제거 및 표준화
    config_col_df = config_col_df.copy()
    config_col_df.columns = config_col_df.columns.str.strip()
    # 중간 공백 있는 열 이름 표준화
    col_rename = {
        '표준 칼럼명' : '표준칼럼명',
        '원본 칼럼명' : '원본칼럼명',
        '표준칼럼명'  : '표준칼럼명',
    }
    config_col_df = config_col_df.rename(columns=col_rename)
    df = df.copy()
    df.columns = df.columns.str.strip()

    media_config = config_col_df[config_col_df['매체명'].str.strip() == str(media_name).strip()]
    result_df    = pd.DataFrame()

    # 디버깅 : 실제 열 이름 출력
    st.write(f"[DEBUG] Config_Column 열 이름: {list(config_col_df.columns)}")
    st.write(f"[DEBUG] 매체명 목록: {list(config_col_df['매체명'].unique()) if '매체명' in config_col_df.columns else '매체명 열 없음'}")
    st.write(f"[DEBUG] media_config 행수: {len(media_config)}")

    for _, row in media_config.iterrows():
        std_col    = str(row['표준칼럼명']).strip()
        proc_type  = str(row['가공유형']).strip()
        src_col    = str(row['원본칼럼명']).strip() if pd.notna(row['원본칼럼명']) else ''
        param      = str(row['파라미터']).strip()  if pd.notna(row['파라미터'])  else ''

        try:
            # 원본 동일 매핑
            if proc_type == 'map':
                result_df[std_col] = df[src_col]

            # 인덱스 위치로 매핑
            elif proc_type == 'map_idx':
                result_df[std_col] = df.iloc[:, int(src_col)]

            # 고정값
            elif proc_type == 'static':
                try:
                    value = float(param) if '.' in str(param) else int(param)
                except:
                    value = param
                result_df[std_col] = value

            # 날짜 형식 변환
            elif proc_type == 'date_format':
                try:
                    result_df[std_col] = pd.to_datetime(
                        df[src_col], format=param
                    ).dt.date
                except:
                    # 형식이 혼재된 경우 자동 파싱
                    result_df[std_col] = pd.to_datetime(
                        df[src_col], format='mixed'
                    ).dt.date

            # 날짜 파생 열 생성
            elif proc_type == 'date_extract':
                base = pd.to_datetime(result_df['date'])
                if param == 'week':
                    result_df[std_col] = base.apply(
                        lambda x: f"{x.month:02d}/{x.isocalendar()[1] - pd.Timestamp(x.year, x.month, 1).isocalendar()[1] + 1:02d}주차"
                    )
                elif param == 'month':
                    result_df[std_col] = base.dt.month.apply(lambda x: f"{x:02d}월")
                elif param == 'year':
                    result_df[std_col] = base.dt.year.apply(lambda x: f"{x}년")

            # 열 병합
            elif proc_type == 'concat':
                parts  = param.split('|')
                result = None
                for part in parts:
                    val = df[part] if part in df.columns else part
                    result = val if result is None else result + val
                result_df[std_col] = result

            # 조건 분기 병합
            elif proc_type == 'concat_if':
                # 파라미터 형식: 키워드|매칭시수식|기본수식
                # concat 과 동일 구조, custom 으로 처리 권장
                pass

            # 숫자 변환
            elif proc_type == 'to_numeric':
                result_df[std_col] = pd.to_numeric(
                    df[src_col].apply(
                        lambda x: str(x).replace(',', '')
                    ), errors='coerce'
                ).fillna(0)

            # 열 합산
            elif proc_type == 'sum_cols':
                cols = param.split('|')
                result_df[std_col] = result_df[cols].apply(
                    pd.to_numeric, errors='coerce'
                ).fillna(0).sum(axis=1)

            # 단일 키 lookup
            elif proc_type == 'lookup':
                sheet, key_col, val_col = param.split('|')
                ref_df  = config_sheets[sheet]
                mapping = ref_df.set_index(key_col)[val_col]
                result_df[std_col] = result_df[key_col].map(mapping)

            # 복합 키 lookup
            elif proc_type == 'lookup_multi':
                sheet, keys, val_col = param.split('|')
                key_cols = keys.split('+')
                ref_df   = config_sheets[sheet].copy()
                ref_df['_key']       = ref_df[key_cols].astype(str).agg('_'.join, axis=1)
                result_df['_key']    = result_df[key_cols].astype(str).agg('_'.join, axis=1)
                mapping              = ref_df.set_index('_key')[val_col]
                result_df[std_col]   = result_df['_key'].map(mapping)
                result_df            = result_df.drop(columns=['_key'])

            # custom 함수
            elif proc_type == 'custom':
                func = custom_functions.get(param)
                if func is None:
                    st.warning(f"custom 함수 '{param}' 를 찾을 수 없습니다.")
                    continue

                # 함수별 인자 분기
                if param == 'extract_ad_code':
                    df = func(df, config_sheets.get('Condition', pd.DataFrame()))
                    result_df[std_col] = df[std_col]
                elif param == 'af_event_count':
                    df = func(df, std_col)
                    result_df[std_col] = df[std_col]
                elif param == 'af_revenue':
                    df = func(df)
                    result_df[std_col] = df[std_col]
                elif param == 'ga4_index':
                    df = func(df)
                    result_df[std_col] = df[std_col]
                elif param == 'naver_bsa_daily':
                    bsa_cost_df = config_sheets.get('Config_BSAcost', pd.DataFrame())
                    df = func(df, bsa_cost_df, year_month)
                    result_df = df
                elif param == 'extract_new_codes':
                    pass  # STEP 1 전용

        except Exception as e:
            st.warning(f"[{media_name}] {std_col} 처리 중 오류 : {e}")

    return result_df


def to_excel_bytes(df_dict):
    """딕셔너리 {시트명: df} 를 엑셀 바이트로 변환"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in df_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()


# =====================
# 파일 업로드 영역
# =====================
st.sidebar.header("📁 파일 업로드")

condition_file = st.sidebar.file_uploader(
    "① 설정 파일 업로드 [(ADEF)HFM_report_condition]",
    type=['xlsx'],
    key='condition'
)

media_files = st.sidebar.file_uploader(
    "② 매체 원본 파일 업로드 (여러 파일 한번에 선택 가능)",
    type=['xlsx', 'csv'],
    accept_multiple_files=True,
    key='media'
)

year_month = st.sidebar.text_input(
    "③ 처리 연월 입력 (YYYY-MM)",
    placeholder="예) 2026-03"
)

st.sidebar.caption("💡 Ctrl 또는 Cmd 누른 채로 클릭하면 여러 파일 동시 선택 가능")

# =====================
# 설정 파일 로드
# =====================
if not condition_file:
    st.info("👈 좌측에서 설정 파일과 매체 파일을 업로드해주세요.")
    st.stop()

config_sheets  = load_excel_sheets(condition_file)
config_file_df = config_sheets.get('Config_File',   pd.DataFrame())
config_col_df  = config_sheets.get('Config_Column', pd.DataFrame())
condition_df   = config_sheets.get('Condition',     pd.DataFrame())

if config_file_df.empty or config_col_df.empty:
    st.error("설정 파일에 Config_File 또는 Config_Column 시트가 없습니다.")
    # 디버깅용 : 실제 시트명 출력
    st.error(f"현재 감지된 시트명 : {list(config_sheets.keys())}")
    st.stop()

# 모든 시트 열 이름 공백 제거 (앞뒤 공백으로 인한 KeyError 방지)
for sheet_name in list(config_sheets.keys()):
    config_sheets[sheet_name].columns = config_sheets[sheet_name].columns.str.strip()

config_file_df = config_sheets.get('Config_File',   pd.DataFrame())
config_col_df  = config_sheets.get('Config_Column', pd.DataFrame())
condition_df   = config_sheets.get('Condition',     pd.DataFrame())

# 필수 열 확인
required_cols = ['매체명', '파일형식', '헤더행', '파일명패턴']
missing_cols  = [c for c in required_cols if c not in config_file_df.columns]
if missing_cols:
    st.error(f"Config_File 시트에 다음 열이 없습니다 : {missing_cols}")
    st.error(f"현재 감지된 열 : {list(config_file_df.columns)}")
    st.stop()

if not media_files:
    st.info("👈 좌측에서 매체 원본 파일을 업로드해주세요.")
    st.stop()

# 매체 파일 → 파일명으로 매체명 매칭
media_file_map = {}
for f in media_files:
    for _, cfg_row in config_file_df.iterrows():
        raw_pattern = str(cfg_row['파일명패턴']).strip()
        # * 를 .* 로 변환, 괄호 등 특수문자 처리
        pattern = re.sub(r'\*', '.*', re.escape(raw_pattern).replace(r'\*', '.*'))
        if re.search(pattern, f.name, re.IGNORECASE):
            media_file_map[cfg_row['매체명']] = f
            break

st.sidebar.markdown("---")
st.sidebar.subheader("📋 파일 감지 현황")

# 업로드된 파일명 표시
if media_files:
    st.sidebar.caption("업로드된 파일:")
    for f in media_files:
        st.sidebar.caption(f"· {f.name}")
    st.sidebar.markdown("---")

for _, cfg_row in config_file_df.iterrows():
    media = cfg_row['매체명']
    if media in media_file_map:
        st.sidebar.success(f"✅ {media}")
    else:
        st.sidebar.error(f"🔴 {media} (파일 없음 / 패턴 미매칭)")

# =====================
# STEP 1 : 광고코드 추출 및 신규 확인
# =====================
st.header("STEP 1 : 광고코드 추출 및 신규 확인")
st.caption("""
1단계 : 인덱스값에서 광고코드 패턴(HF/TM/TJ + 영문2자 + 숫자4자리) 추출 → 없으면 '확인필요'
2단계 : Code 시트와 대조 → 기존 인덱스 제외, 신규만 추출
""")

# 광고코드 패턴 추출 함수
def extract_code_from_index(index_val):
    """인덱스 문자열에서 광고코드 패턴 추출"""
    pattern = r'(HF|TM|TJ)[A-Z]{2}\d{4}'
    match   = re.search(pattern, str(index_val))
    return match.group() if match else '확인필요'

if st.button("▶ 광고코드 추출 실행", key='step1'):

    code_map = {
        'Code_media' : '인덱스(매체)',
        'Code_ga4'   : '인덱스(ga4)',
        'Code_af'    : '인덱스(AF)',
    }

    result_sheets = {}

    for media_name, media_file in media_file_map.items():
        cfg_row   = get_config_row(config_file_df, media_name)
        df        = read_media_file(media_file, cfg_row)
        processed = apply_config_column(
            df, config_col_df, media_name, config_sheets, year_month
        )

        for sheet_name, index_col in code_map.items():
            if index_col not in processed.columns:
                continue

            # 고유 인덱스 목록
            all_indexes = processed[index_col].dropna().unique()

            # 1단계 : 인덱스에서 광고코드 패턴 추출
            extracted = pd.DataFrame({
                index_col  : all_indexes,
                '광고코드' : [extract_code_from_index(idx) for idx in all_indexes],
            })

            # 2단계 : Code 시트와 대조 → 기존 인덱스 제외
            code_df = config_sheets.get(sheet_name, pd.DataFrame())
            if not code_df.empty and index_col in code_df.columns:
                existing_indexes = set(code_df[index_col].dropna().unique())
            else:
                existing_indexes = set()

            new_df      = extracted[~extracted[index_col].isin(existing_indexes)].copy()
            existing_df = extracted[extracted[index_col].isin(existing_indexes)].copy()

            # 신규 중 확인필요 건
            need_check = (new_df['광고코드'] == '확인필요').sum()
            new_count  = len(new_df)
            exist_count = len(existing_df)

            # 결과 요약
            st.markdown(f"**{media_name} / {sheet_name}**")
            col1, col2, col3 = st.columns(3)
            col1.metric("기존 인덱스", f"{exist_count}건")
            col2.metric("신규 인덱스", f"{new_count}건")
            col3.metric("확인필요", f"{need_check}건")

            if need_check > 0:
                st.warning(f"⚠️ 광고코드 패턴 미확인 {need_check}건 포함")
            elif new_count > 0:
                st.info(f"🔍 신규 인덱스 {new_count}건 발견")
            else:
                st.success(f"✅ 신규 인덱스 없음")

            # 신규 목록 인라인 표시
            if not new_df.empty:
                with st.expander(f"신규 목록 보기 ({media_name} / {sheet_name})"):
                    st.dataframe(
                        new_df.sort_values(index_col).reset_index(drop=True),
                        use_container_width=True
                    )

            # 신규 건만 결과 시트에 저장
            if not new_df.empty:
                result_sheets[f"{sheet_name}_{media_name}"] = (
                    new_df.sort_values(index_col).reset_index(drop=True)
                )

    if result_sheets:
        excel_bytes = to_excel_bytes(result_sheets)
        st.download_button(
            label="⬇ 신규 광고코드 엑셀 다운로드",
            data=excel_bytes,
            file_name=f"(ADEF)신규광고코드_{year_month.replace('-','')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.success("✅ 전체 매체 신규 코드 없음")

st.divider()

# =====================
# STEP 2 : RD 생성
# =====================
st.header("STEP 2 : 매체 RD 전처리 및 추출")
st.caption("설정 파일 기준으로 전처리 후 통합 RD를 생성합니다.")

if st.button("▶ RD 생성 실행", key='step2'):
    if not year_month:
        st.error("처리 연월을 입력해주세요. (예: 2026-03)")
        st.stop()

    all_dfs = []

    for media_name, media_file in media_file_map.items():
        with st.spinner(f"{media_name} 처리 중..."):
            try:
                cfg_row = get_config_row(config_file_df, media_name)
                df      = read_media_file(media_file, cfg_row)

                processed = apply_config_column(
                    df, config_col_df, media_name, config_sheets, year_month
                )
                processed['매체명'] = media_name
                all_dfs.append(processed)
                st.success(f"✅ {media_name} : {len(processed)}행 처리 완료")

            except Exception as e:
                st.error(f"❌ {media_name} 처리 실패 : {e}")

    if not all_dfs:
        st.error("처리된 데이터가 없습니다.")
        st.stop()

    # 전체 통합
    final_df = pd.concat(all_dfs, ignore_index=True)

    # 프리뷰
    st.subheader("📋 전처리 결과 프리뷰 (상위 30행)")
    st.dataframe(final_df.head(30), use_container_width=True)
    st.caption(f"전체 : {len(final_df)}행 / {len(final_df.columns)}열")

    # 다운로드
    excel_bytes = to_excel_bytes({"RD": final_df})
    st.download_button(
        label="⬇ RD 엑셀 다운로드",
        data=excel_bytes,
        file_name=f"(ADEF)HFM_RD_{year_month.replace('-','')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
