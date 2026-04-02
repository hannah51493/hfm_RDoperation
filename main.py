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
    file_format = config_row['파일형식'].lower().strip()

    # 실제 파일명 확장자로 재확인
    filename = file.name.lower() if hasattr(file, 'name') else ''

    if file_format == 'csv' or filename.endswith('.csv'):
        # chardet 으로 인코딩 자동 감지
        df = None
        try:
            import chardet, io
            file.seek(0)
            raw      = file.read()
            detected = chardet.detect(raw)
            encoding = detected.get('encoding', 'cp949') or 'cp949'

            # 탭 구분자 여부 감지
            sample = raw[:2000].decode(encoding, errors='ignore')
            sep    = '	' if sample.count('	') > sample.count(',') else ','

            df = pd.read_csv(io.BytesIO(raw), header=header_row, encoding=encoding, sep=sep)
        except Exception:
            for encoding in ['utf-8-sig', 'cp949', 'euc-kr', 'utf-8', 'latin1']:
                for sep in ['	', ',']:
                    try:
                        file.seek(0)
                        df = pd.read_csv(file, header=header_row, encoding=encoding, sep=sep)
                        if len(df.columns) > 1:
                            break
                    except Exception:
                        continue
                if df is not None and len(df.columns) > 1:
                    break
        if df is None:
            raise ValueError(f"파일 인코딩을 인식할 수 없습니다: {filename}")
    elif filename.endswith('.xls'):
        df = pd.read_excel(file, header=header_row, engine='xlrd')
    elif filename.endswith('.xlsx'):
        df = pd.read_excel(file, header=header_row, engine='openpyxl')
    else:
        # 확장자 불명확 시 자동 감지
        try:
            df = pd.read_excel(file, header=header_row, engine='openpyxl')
        except:
            try:
                df = pd.read_excel(file, header=header_row, engine='xlrd')
            except:
                df = pd.read_csv(file, header=header_row)

    return df


# 가공유형 처리 순서
PROC_ORDER = [
    'map', 'map_idx', 'static', 'date_format', 'to_numeric',
    'concat',
    'custom_pre',    # lookup 전 실행 (ga4_index 등)
    'date_extract',
    'lookup',        # 광고코드 생성
    'custom',        # lookup 후 실행 (naver_bsa_daily, af_event_count 등)
    'lookup_multi',  # GA4/AF 데이터 매핑
    'custom_post',   # lookup_multi 후 실행 (meta_purchase, meta_revenue 등)
    'sum_cols',
]

# custom_pre : lookup 이전 실행
CUSTOM_PRE_FUNCS = ['ga4_index']

# custom_post : lookup_multi 이후 실행
CUSTOM_POST_FUNCS = ['meta_purchase', 'meta_revenue']

def apply_config_column(df, config_col_df, media_name, config_sheets, year_month=None):
    """
    Config_Column 시트 기준으로 가공유형 순서대로 적용
    """
    # 열 이름 공백 제거 및 표준화
    config_col_df = config_col_df.copy()
    config_col_df.columns = config_col_df.columns.str.strip()
    config_col_df = config_col_df.rename(columns={
        '표준 칼럼명': '표준칼럼명',
        '원본 칼럼명': '원본칼럼명',
    })
    df = df.copy()
    df.columns = df.columns.str.strip()

    media_config = config_col_df[
        config_col_df['매체명'].str.strip() == str(media_name).strip()
    ].copy()

    # 인덱스 리셋 (Reindexing 오류 방지)
    df = df.reset_index(drop=True)

    # result_df : 표준칼럼 저장용 (인덱스 없이 초기화)
    result_df = pd.DataFrame()

    def get_val(row, key):
        v = row.get(key, '')
        return str(v).strip() if pd.notna(v) else ''

    # 가공유형 순서대로 실행
    for proc_type in PROC_ORDER:
        # custom 계열 파라미터로 단계 구분
        if proc_type == 'custom_pre':
            rows = media_config[
                (media_config['가공유형'].str.strip() == 'custom') &
                (media_config['파라미터'].str.strip().isin(CUSTOM_PRE_FUNCS))
            ]
        elif proc_type == 'custom_post':
            rows = media_config[
                (media_config['가공유형'].str.strip() == 'custom') &
                (media_config['파라미터'].str.strip().isin(CUSTOM_POST_FUNCS))
            ]
        elif proc_type == 'custom':
            rows = media_config[
                (media_config['가공유형'].str.strip() == 'custom') &
                (~media_config['파라미터'].str.strip().isin(CUSTOM_PRE_FUNCS)) &
                (~media_config['파라미터'].str.strip().isin(CUSTOM_POST_FUNCS))
            ]
        else:
            rows = media_config[media_config['가공유형'].str.strip() == proc_type]

        for _, row in rows.iterrows():
            std_col  = get_val(row, '표준칼럼명')
            src_col  = get_val(row, '원본칼럼명')
            param    = get_val(row, '파라미터')

            try:
                if proc_type == 'map':
                    if src_col in df.columns:
                        result_df[std_col] = df[src_col].values

                elif proc_type == 'map_idx':
                    result_df[std_col] = df.iloc[:, int(src_col)].values

                elif proc_type == 'static':
                    try:
                        value = float(param) if '.' in param else int(param)
                    except:
                        value = param
                    result_df[std_col] = value

                elif proc_type == 'date_format':
                    try:
                        result_df[std_col] = pd.to_datetime(
                            df[src_col], format=param
                        ).dt.date
                    except:
                        result_df[std_col] = pd.to_datetime(
                            df[src_col], format='mixed'
                        ).dt.date

                elif proc_type == 'to_numeric':
                    result_df[std_col] = pd.to_numeric(
                        df[src_col].astype(str).str.replace(',', ''),
                        errors='coerce'
                    ).fillna(0).values

                elif proc_type == 'concat':
                    parts  = param.split('|')
                    result = None
                    for part in parts:
                        # 표준칼럼 먼저, 없으면 원본, 없으면 문자 그대로
                        if part in result_df.columns:
                            val = result_df[part].astype(str)
                        elif part in df.columns:
                            val = df[part].astype(str)
                        else:
                            val = part
                        result = val if result is None else result + val
                    result_df[std_col] = result

                elif proc_type in ('custom', 'custom_pre', 'custom_post'):
                    func = custom_functions.get(param)
                    if func is None:
                        st.warning(f"custom 함수 '{param}' 없음")
                        continue

                    # custom_pre 단계: CUSTOM_PRE_FUNCS 만 실행
                    if proc_type == 'custom_pre' and param not in CUSTOM_PRE_FUNCS:
                        continue
                    # custom 단계: CUSTOM_PRE_FUNCS / CUSTOM_POST_FUNCS 제외
                    if proc_type == 'custom' and (param in CUSTOM_PRE_FUNCS or param in CUSTOM_POST_FUNCS):
                        continue
                    # custom_post 단계: CUSTOM_POST_FUNCS 만 실행
                    if proc_type == 'custom_post' and param not in CUSTOM_POST_FUNCS:
                        continue

                    if param == 'ga4_index':
                        merged = pd.concat([df, result_df], axis=1).reset_index(drop=True)
                        merged = func(merged)
                        result_df[std_col] = merged[std_col].values

                    elif param == 'naver_bsa_daily':
                        bsa_cost_df = config_sheets.get('Config_BSAcost', pd.DataFrame())
                        df = func(df, bsa_cost_df, year_month).reset_index(drop=True)
                        result_df = result_df.reset_index(drop=True)
                        result_df[std_col] = df['spent'].values

                    elif param == 'ga4_index':
                        merged = pd.concat([df, result_df], axis=1)
                        merged = func(merged)
                        result_df[std_col] = merged['인덱스(ga4)'].values

                    elif param == 'af_event_count':
                        merged = pd.concat([df, result_df], axis=1).reset_index(drop=True)
                        merged = func(merged, std_col)
                        result_df[std_col] = merged[std_col].values

                    elif param == 'af_revenue':
                        merged = pd.concat([df, result_df], axis=1)
                        merged = func(merged)
                        result_df[std_col] = merged['(AF)revenue'].values

                    elif param == 'extract_new_codes':
                        pass

                    else:
                        # 일반 custom 함수 처리
                        merged = pd.concat([df, result_df], axis=1).reset_index(drop=True)
                        merged = merged.loc[:, ~merged.columns.duplicated(keep='last')]
                        merged = func(merged)
                        if std_col in merged.columns:
                            result_df[std_col] = merged[std_col].values
                        else:
                            st.warning(f"[{media_name}] {std_col} : {param} 실행 후 열 없음")

                elif proc_type == 'date_extract':
                    date_col = 'date' if 'date' in result_df.columns else ('Date' if 'Date' in result_df.columns else None)
                    if date_col is None:
                        continue
                    base = pd.to_datetime(result_df[date_col], errors='coerce')
                    if param == 'week':
                        def safe_week(x):
                            try:
                                # 해당 날짜가 속한 주의 월요일 계산
                                monday = x - pd.Timedelta(days=x.weekday())
                                return f"{monday.month:02d}/{monday.day:02d}주차"
                            except:
                                return ''
                        result_df[std_col] = base.apply(safe_week)
                    elif param == 'month':
                        result_df[std_col] = base.apply(lambda x: f"{x.month:02d}월" if pd.notna(x) else '')
                    elif param == 'year':
                        result_df[std_col] = base.apply(lambda x: f"{x.year}년" if pd.notna(x) else '')

                elif proc_type == 'lookup':
                    sheet, key_col, val_col = param.split('|')
                    ref_df = config_sheets.get(sheet, pd.DataFrame())
                    if not ref_df.empty and key_col in ref_df.columns:
                        # 광고코드 있는 행 우선 (- 또는 빈값보다 실제 코드 우선)
                        ref_sorted = ref_df.copy()
                        ref_sorted['_has_code'] = ref_sorted[val_col].apply(
                            lambda x: 0 if str(x).strip() in ['-', '', 'nan', 'None'] else 1
                        )
                        ref_sorted = ref_sorted.sort_values('_has_code', ascending=False).drop(columns=['_has_code'])
                        mapping = ref_sorted.drop_duplicates(key_col).set_index(key_col)[val_col]
                        result_df[std_col] = result_df[key_col].map(mapping)

                elif proc_type == 'lookup_multi':
                    sheet, keys, val_col = param.split('|')
                    key_cols = keys.split('+')
                    ref_df   = config_sheets.get(sheet, pd.DataFrame())
                    if not ref_df.empty:
                        missing = [k for k in key_cols if k not in ref_df.columns]
                        if not missing:
                            # 키 샘플 비교 디버그
                            if sheet == 'appsflyer' and val_col == '(AF)install':
                                # 실제 매칭 건수 확인
                                test_result = result_df[key_cols].astype(str).agg('_'.join, axis=1)
                                test_af     = ref_df[key_cols].astype(str).agg('_'.join, axis=1)
                                matched = test_result.isin(test_af).sum()
                            ref_df    = ref_df.copy().reset_index(drop=True)
                            result_df = result_df.reset_index(drop=True)
                            # 날짜 타입 통일 (str 로 변환해서 비교)
                            for k in key_cols:
                                ref_df[k]    = ref_df[k].astype(str).str.strip()
                                result_df[k] = result_df[k].astype(str).str.strip()
                            ref_df['_key']     = ref_df[key_cols].agg('_'.join, axis=1)
                            result_df['_key']  = result_df[key_cols].agg('_'.join, axis=1)
                            mapping            = ref_df.drop_duplicates('_key').set_index('_key')[val_col]
                            result_df[std_col] = result_df['_key'].map(mapping)
                            result_df          = result_df.drop(columns=['_key'])

                elif proc_type == 'sum_cols':
                    cols = param.split('|')
                    avail = [c for c in cols if c in result_df.columns]
                    if avail:
                        result_df[std_col] = result_df[avail].apply(
                            pd.to_numeric, errors='coerce'
                        ).fillna(0).sum(axis=1)

            except Exception as e:
                st.warning(f"[{media_name}] {std_col} ({proc_type}) 처리 중 오류 : {e}")

    # Spent 계산 (spent_type 기준)
    if 'Spent' in result_df.columns and not result_df['Spent'].isnull().all():
        cfg_file_df = config_sheets.get('Config_File', pd.DataFrame())
        if not cfg_file_df.empty:
            cfg_file_df.columns = cfg_file_df.columns.str.strip()
            media_cfg_row = cfg_file_df[cfg_file_df['매체명'].str.strip() == str(media_name).strip()]
            if not media_cfg_row.empty:
                row        = media_cfg_row.iloc[0]
                spent_type = str(row.get('spent_type', 'A')).strip()
                markup     = pd.to_numeric(row.get('markup_rate', 1), errors='coerce') or 1
                exchange   = pd.to_numeric(row.get('exchange_rate', 1), errors='coerce') or 1
                vat        = pd.to_numeric(row.get('vat', 1), errors='coerce') or 1

                spent = pd.to_numeric(result_df['Spent'], errors='coerce').fillna(0)

                if spent_type == 'A':
                    result_df['Spent'] = spent
                elif spent_type == 'B':
                    result_df['Spent'] = (spent / vat).round(0)
                elif spent_type == 'C':
                    result_df['Spent'] = (spent * markup).round(0)
                elif spent_type == 'D':
                    result_df['Spent'] = (spent * markup * exchange).round(0)
                # E (Naver BSA) 는 naver_bsa_daily 에서 이미 처리됨

    # 표준칼럼 순서대로 필터링해서 반환
    std_cols   = list(media_config['표준칼럼명'].str.strip())
    final_cols = [c for c in std_cols if c in result_df.columns]
    return result_df[final_cols] if final_cols else result_df


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

# 매체 파일 없어도 계속 진행 (업로드된 파일만 처리)

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
# 탭 구성
# =====================
tab1, tab2 = st.tabs(["🔍 STEP 1 : 광고코드 추출", "📊 STEP 2 : RD 생성"])

# =====================
# STEP 1
# =====================
with tab1:
    st.header("STEP 1 : 광고코드 추출 및 신규 확인")
    st.caption("인덱스값에서 광고코드 패턴 추출 → Code 시트와 대조 → 신규만 추출")

    def extract_codes(index_val):
        full_matches = re.findall(r'(?:HF|TM|TJ)[A-Z]{2}\d{4}', str(index_val))
        return ', '.join(full_matches) if full_matches else '확인필요'

    if st.button("▶ 광고코드 추출 실행", key='step1'):
        file_index_map = {
            'GA4'       : ('Code_ga4', '인덱스(ga4)'),
            'Appsflyer' : ('Code_af',  '인덱스(AF)'),
        }
        result_sheets = {}

        for media_name, media_file in media_file_map.items():
            cfg_row  = get_config_row(config_file_df, media_name)
            df       = read_media_file(media_file, cfg_row)
            df.columns = df.columns.str.strip()

            if media_name in file_index_map:
                sheet_name, index_col = file_index_map[media_name]
            else:
                sheet_name, index_col = 'Code_media', '인덱스(매체)'

            media_cfg = config_col_df[config_col_df['매체명'].str.strip() == media_name.strip()].copy()
            temp_df   = df.copy()

            # 1차: map 먼저
            for _, row in media_cfg.iterrows():
                std_col   = str(row['표준칼럼명']).strip()
                proc_type = str(row['가공유형']).strip()
                src_col   = str(row['원본칼럼명']).strip() if pd.notna(row['원본칼럼명']) else ''
                param     = str(row['파라미터']).strip() if pd.notna(row['파라미터']) else ''
                try:
                    if proc_type == 'map' and src_col in temp_df.columns:
                        temp_df[std_col] = temp_df[src_col]
                    elif proc_type == 'map_idx':
                        temp_df[std_col] = temp_df.iloc[:, int(src_col)]
                    elif proc_type == 'static':
                        try: val = float(param) if '.' in param else int(param)
                        except: val = param
                        temp_df[std_col] = val
                    elif proc_type == 'date_format':
                        try: temp_df[std_col] = pd.to_datetime(temp_df[src_col], format=param).dt.date
                        except: temp_df[std_col] = pd.to_datetime(temp_df[src_col], format='mixed').dt.date
                except: pass

            # 2차: 인덱스 열 생성 (이미 1차에서 map 으로 생성된 경우 스킵)
            if index_col not in temp_df.columns:
                for _, row in media_cfg[media_cfg['표준칼럼명'].str.strip() == index_col].iterrows():
                    proc_type = str(row['가공유형']).strip()
                    param     = str(row['파라미터']).strip() if pd.notna(row['파라미터']) else ''
                    try:
                        if proc_type == 'concat':
                            parts = param.split('|')
                            result = None
                            for part in parts:
                                if part in temp_df.columns: val = temp_df[part].astype(str)
                                else: val = part
                                result = val if result is None else result + val
                            temp_df[index_col] = result
                        elif proc_type == 'map' and str(row['원본칼럼명']).strip() in temp_df.columns:
                            temp_df[index_col] = temp_df[str(row['원본칼럼명']).strip()]
                        elif proc_type == 'custom':
                            func = custom_functions.get(param)
                            if func: temp_df = func(temp_df)
                    except Exception as e:
                        st.warning(f"⚠️ {media_name} 인덱스 생성 오류 : {e}")

            if index_col not in temp_df.columns:
                st.warning(f"⚠️ {media_name} : '{index_col}' 열 생성 실패")
                continue

            all_indexes = temp_df[index_col].dropna().unique()
            extracted   = pd.DataFrame({
                index_col  : all_indexes,
                '광고코드' : [extract_codes(idx) for idx in all_indexes],
            })

            code_df = config_sheets.get(sheet_name, pd.DataFrame())
            existing_indexes = set(code_df[index_col].dropna().unique()) if not code_df.empty and index_col in code_df.columns else set()

            new_df      = extracted[~extracted[index_col].isin(existing_indexes)].copy()
            existing_df = extracted[extracted[index_col].isin(existing_indexes)].copy()
            need_check  = (new_df['광고코드'] == '확인필요').sum()

            st.markdown(f"**{media_name} / {sheet_name}**")
            c1, c2, c3 = st.columns(3)
            c1.metric("기존 인덱스", f"{len(existing_df)}건")
            c2.metric("신규 인덱스", f"{len(new_df)}건")
            c3.metric("확인필요",   f"{need_check}건")

            if need_check > 0: st.warning(f"⚠️ 광고코드 패턴 미확인 {need_check}건 포함")
            elif len(new_df) > 0: st.info(f"🔍 신규 인덱스 {len(new_df)}건 발견")
            else: st.success("✅ 신규 인덱스 없음")

            if not new_df.empty:
                with st.expander(f"신규 목록 보기 ({media_name} / {sheet_name})"):
                    st.dataframe(new_df.sort_values(index_col).reset_index(drop=True), use_container_width=True)
                if sheet_name not in result_sheets:
                    result_sheets[sheet_name] = []
                result_sheets[sheet_name].append(new_df.sort_values(index_col).reset_index(drop=True))

        if result_sheets:
            final_sheets = {k: pd.concat(v, ignore_index=True) for k, v in result_sheets.items()}
            st.download_button(
                label="⬇ 신규 광고코드 엑셀 다운로드",
                data=to_excel_bytes(final_sheets),
                file_name=f"(ADEF)신규광고코드_{year_month.replace('-','')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.success("✅ 전체 매체 신규 코드 없음")

# =====================
# STEP 2
# =====================
with tab2:
    st.header("STEP 2 : 매체 RD 전처리 및 추출")
    st.caption("설정 파일 기준으로 전처리 후 통합 RD를 생성합니다.")

    TRACKER_MEDIA     = ['GA4', 'Appsflyer']
    available_media   = [m for m in media_file_map.keys() if m not in TRACKER_MEDIA]

    if not available_media:
        st.info("👈 좌측에서 매체 원본 파일을 업로드해주세요.")
        selected_media = []
    else:
        select_all = st.checkbox("전체 매체 선택", value=True)
        selected_media = available_media if select_all else st.multiselect(
            "추출할 매체 선택 (복수 선택 가능)",
            options=available_media, default=available_media
        )

    STANDARD_COL_ORDER = [
        '인덱스(매체)', '광고코드', '주체', 'Date', 'Week', 'Month', 'Year',
        '대구분', '구분', '미디어', '광고상품', '광고유형', '캠페인목표',
        '캠페인', '프로모션', '타겟팅', '상세컨텐츠', '브랜드', '상태',
        'Spent', 'Impression', 'Clicks', 'Link Click',
        '세션', '사용자',
        '구매', '구매(WEB)', '구매(APP)',
        '매출액', '매출액(WEB)', '매출액(APP)',
        '회원가입', '회원가입(WEB)', '회원가입(APP)',
        '앱설치',
        '구매(매체)', '매출액(매체)', '회원가입(매체)', '앱설치(매체)',
        '앱설치 (AF)', '재설치 (AF)', '인게이지먼트 (AF)', '구매 (AF)', '매출액 (AF)', '회원가입(AF)',
        '매체명'
    ]

    with st.expander("⚙️ 추출 컬럼 선택 (선택 안 하면 전체 추출)"):
        col_filter = st.multiselect("제외할 컬럼 선택", options=STANDARD_COL_ORDER, default=[])

    if st.button("▶ RD 생성 실행", key='step2'):
        if not year_month:
            st.error("처리 연월을 입력해주세요. (예: 2026-03)")
            st.stop()
        if not selected_media:
            st.warning("추출할 매체를 선택하거나 파일을 업로드해주세요.")
            st.stop()

        # GA4 / AF 먼저 전처리 → config_sheets 에 저장
        for tracker_name, sheet_key in {'GA4': 'ga4', 'Appsflyer': 'appsflyer'}.items():
            if tracker_name in media_file_map:
                try:
                    t_df        = read_media_file(media_file_map[tracker_name], get_config_row(config_file_df, tracker_name))
                    t_processed = apply_config_column(t_df, config_col_df, tracker_name, config_sheets, year_month).reset_index(drop=True)
                    # date 열 str 로 통일 (lookup_multi 키 매칭용)
                    if 'Date' in t_processed.columns:
                        t_processed['Date'] = t_processed['Date'].astype(str).str.strip()
                    # GA4 / AF : Date + 광고코드 기준으로 그룹화 합산 (SUMIF 방식)
                    if '광고코드' in t_processed.columns and 'Date' in t_processed.columns:
                        t_processed['Date']     = t_processed['Date'].astype(str).str.strip()
                        t_processed['광고코드'] = t_processed['광고코드'].astype(str).str.strip()

                        # 숫자 열 전부 합산 (모든 이벤트 수 + 매출 포함)
                        numeric_cols = t_processed.select_dtypes(include='number').columns.tolist()
                        group_cols   = ['Date', '광고코드']

                        # 숫자 열 먼저 to_numeric 처리
                        for col in numeric_cols:
                            t_processed[col] = pd.to_numeric(t_processed[col], errors='coerce').fillna(0)

                        # 광고코드 '-' 는 실제 코드가 있는 행이 있으면 제외
                        # (동일 인덱스에 '-' 와 실제코드 중복 시 실제코드 우선)
                        if numeric_cols:
                            t_grouped = t_processed.groupby(group_cols, as_index=False)[numeric_cols].sum()
                        else:
                            t_grouped = t_processed.drop_duplicates(group_cols)

                        config_sheets[sheet_key] = t_grouped.reset_index(drop=True)
                        st.info(f"✅ {tracker_name} 전처리 완료 : {len(t_processed)}행 → 그룹화 후 {len(t_grouped)}행")
                        if tracker_name == 'Appsflyer':
                            dup = t_grouped.duplicated(['Date', '광고코드']).sum()
                    else:
                        config_sheets[sheet_key] = t_processed.reset_index(drop=True)
                        st.info(f"✅ {tracker_name} 전처리 완료 : {len(t_processed)}행")
                except Exception as e:
                    st.warning(f"⚠️ {tracker_name} 전처리 실패 : {e}")

        all_dfs = []
        for media_name, media_file in media_file_map.items():
            if media_name not in selected_media:
                continue
            with st.spinner(f"{media_name} 처리 중..."):
                try:
                    df        = read_media_file(media_file, get_config_row(config_file_df, media_name))
                    processed = apply_config_column(df, config_col_df, media_name, config_sheets, year_month)
                    processed['매체명'] = media_name
                    all_dfs.append(processed)
                    st.success(f"✅ {media_name} : {len(processed)}행 처리 완료")
                except Exception as e:
                    st.error(f"❌ {media_name} 처리 실패 : {e}")

        if not all_dfs:
            st.error("처리된 데이터가 없습니다.")
            st.stop()

        final_df     = pd.concat(all_dfs, ignore_index=True).reset_index(drop=True)
        ordered_cols = [c for c in STANDARD_COL_ORDER if c in final_df.columns and c not in col_filter]
        extra_cols   = [c for c in final_df.columns if c not in STANDARD_COL_ORDER and c not in col_filter]
        final_df     = final_df[ordered_cols + extra_cols]

        st.subheader("📋 전처리 결과 프리뷰 (상위 30행)")
        st.dataframe(final_df.head(30), use_container_width=True)
        st.caption(f"전체 : {len(final_df)}행 / {len(final_df.columns)}열")

        # =====================
        # 데이터 정합성 검증
        # =====================
        st.subheader("🔍 데이터 정합성 검증")
        st.caption("원본 파일 vs 추출 RD 주요 지표 합계 비교 (GA4/AF 제외)")

        verify_cols = ['Impression', 'Clicks', '구매(매체)', '매출액(매체)', '앱설치(매체)']

        verify_rows = []
        verify_gaps = {}  # GAP 컬럼별 값 저장 (스타일링용)

        for media_name, media_file in media_file_map.items():
            if media_name not in selected_media:
                continue
            try:
                cfg_row   = get_config_row(config_file_df, media_name)
                df_raw    = read_media_file(media_file, cfg_row)
                media_cfg = config_col_df[config_col_df['매체명'].str.strip() == media_name.strip()]
                rd_media  = final_df[final_df['매체명'] == media_name]

                row = {'매체명': media_name}
                for std_col in verify_cols:
                    if std_col not in rd_media.columns:
                        continue
                    rd_sum  = pd.to_numeric(rd_media[std_col], errors='coerce').fillna(0).sum()
                    col_cfg = media_cfg[media_cfg['표준칼럼명'].str.strip() == std_col]
                    raw_sum = None
                    if not col_cfg.empty:
                        src_col = str(col_cfg.iloc[0]['원본칼럼명']).strip()
                        if src_col and src_col in df_raw.columns:
                            raw_sum = pd.to_numeric(
                                df_raw[src_col].astype(str).str.replace(',', ''),
                                errors='coerce'
                            ).fillna(0).sum()

                    if raw_sum is not None:
                        gap     = rd_sum - raw_sum
                        gap_pct = gap / raw_sum * 100 if raw_sum != 0 else 0
                        row[f"{std_col}_원본"] = f"{raw_sum:,.0f}"
                        row[f"{std_col}_RD"]   = f"{rd_sum:,.0f}"
                        row[f"{std_col}_GAP%"] = round(gap_pct, 1)
                        verify_gaps[f"{std_col}_GAP%"] = True
                    else:
                        row[f"{std_col}_RD"] = f"{rd_sum:,.0f}"

                verify_rows.append(row)
            except Exception as e:
                verify_rows.append({'매체명': media_name, '오류': str(e)})

        if verify_rows:
            verify_df = pd.DataFrame(verify_rows).set_index('매체명')

            # GAP% 컬럼에 색상 스타일 적용
            gap_cols = [c for c in verify_df.columns if c.endswith('_GAP%')]

            def color_gap(val):
                try:
                    v = float(val)
                    if v == 0:
                        return 'background-color: #d4edda; color: #155724; font-weight: bold'
                    elif abs(v) <= 1:
                        return 'background-color: #fff3cd; color: #856404; font-weight: bold'
                    else:
                        return 'background-color: #f8d7da; color: #721c24; font-weight: bold'
                except:
                    return ''

            styled = verify_df.style
            def make_fmt(c):
                return lambda x: f"{x:+.1f}%" if isinstance(x, (int, float)) else x

            for col in gap_cols:
                try:
                    styled = styled.map(color_gap, subset=[col])
                except AttributeError:
                    styled = styled.applymap(color_gap, subset=[col])
                styled = styled.format({col: make_fmt(col)})

            st.dataframe(styled, use_container_width=True)
            st.caption("🟢 GAP 0%  🟡 GAP ±1% 이내  🔴 GAP ±1% 초과")

        st.download_button(
            label="⬇ RD 엑셀 다운로드",
            data=to_excel_bytes({"RD": final_df}),
            file_name=f"(ADEF)HFM_RD_{year_month.replace('-','')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
