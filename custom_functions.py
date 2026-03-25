import re
import pandas as pd

# =====================
# 신규 광고코드 추출
# =====================
def extract_new_codes(df, code_df, index_col):
    """
    기존 Code 시트와 대조해서 신규 / 미매칭 항목 추출
    df       : 원본 파일 데이터
    code_df  : Code_media / Code_ga4 / Code_af 시트
    index_col: '인덱스(매체)' / '인덱스(ga4)' / '인덱스(AF)'
    """
    # 원본에서 인덱스 고유값 추출
    source_indexes = set(df[index_col].dropna().unique())

    # 기존 코드 시트의 인덱스 목록
    existing_indexes = set(code_df[index_col].dropna().unique())

    # 신규 = 원본에는 있는데 코드 시트에 없는 것
    new_indexes = source_indexes - existing_indexes

    result = pd.DataFrame({
        index_col : list(new_indexes),
        '광고코드' : '',   # 담당자가 채워넣을 열
        '비고'    : '신규'
    })

    return result.sort_values(index_col).reset_index(drop=True)

# =====================
# GA4 인덱스 생성
# =====================
def ga4_index(df):
    def clean_na(val):
        """nan / None / 빈값만 제거 → (not set) 은 그대로 유지"""
        s = str(val).strip()
        if s in ['nan', '', 'None']:
            return ''
        return s

    def clean_src(val):
        """소스매체는 (not set) 포함 빈값 처리 → 조건 분기용"""
        s = str(val).strip()
        if s in ['nan', '(not set)', '', 'None']:
            return ''
        return s

    def build_index(row):
        src      = clean_src(row.get('(ga4)소스매체', ''))
        campaign = clean_na(row.get('(ga4)캠페인', ''))
        keyword  = clean_na(row.get('(ga4)검색어', ''))
        content  = clean_na(row.get('(ga4)컨텐츠', ''))

        # 소스매체 원본값 (not set 포함, 인덱스 조합용)
        src_raw  = clean_na(row.get('(ga4)소스매체', ''))

        if src in ['Apple / search', 'ig / paid']:
            return src_raw + campaign
        elif src == 'google / display' and 'shopping' in campaign.lower():
            return src_raw + campaign + keyword
        else:
            return src_raw + campaign + content

    df['인덱스(ga4)'] = df.apply(build_index, axis=1)
    # 완전히 빈 인덱스만 NaN 처리
    df.loc[df['인덱스(ga4)'] == '', '인덱스(ga4)'] = None
    return df


# =====================
# AF 이벤트 카운트
# =====================
EVENT_MAP = {
    '(AF)install'        : 'install',
    '(AF)purchase'       : 'af_purchase',
    '(AF)re-attribution' : 're-attribution',
    '(AF)re-engagement'  : 're-engagement',
}

def af_event_count(df, target_col):
    event_name = EVENT_MAP[target_col]
    df[target_col] = df['Event Name'].apply(
        lambda x: 1 if x == event_name else 0
    )
    return df


# =====================
# AF 매출 합산
# =====================
def af_revenue(df):
    df['(AF)revenue'] = df.apply(
        lambda x: pd.to_numeric(x['Event Revenue'], errors='coerce') or 0
        if x['Event Name'] == 'af_purchase' else 0,
        axis=1
    ).fillna(0)
    return df

# =====================
# 네이버 브랜드검색 일할 소진금액
# =====================
def naver_bsa_daily(df, bsa_cost_df, year_month):
    """
    Config_BSAcost 시트에서 타겟팅(광고세트) 기준으로 일예산 lookup
    - 원본 파일은 날짜별 행 그대로 사용
    - 소진비용(spent) 만 Config_BSAcost 에서 가져옴
    - year_month : '2026-03' 형식
    """
    month_col = year_month.replace('-', '')

    if month_col not in bsa_cost_df.columns:
        raise ValueError(f"Config_BSAcost 시트에 {month_col} 열이 없습니다.")

    # 타겟팅 → 일예산 매핑 테이블 생성
    bsa_cost_df = bsa_cost_df.copy()
    bsa_cost_df['타겟팅'] = bsa_cost_df['타겟팅'].astype(str).str.strip()

    # 숫자 변환 (콤마 제거)
    bsa_cost_df[month_col] = pd.to_numeric(
        bsa_cost_df[month_col].astype(str).str.replace(',', '').str.strip(),
        errors='coerce'
    ).fillna(0)

    # bsa_cost_df 디버그

    # 중복 타겟팅 제거 후 매핑
    cost_map = bsa_cost_df.drop_duplicates('타겟팅').set_index('타겟팅')[month_col]

    # 원본 df 의 광고세트 기준으로 spent 매핑
    df = df.copy().reset_index(drop=True)
    df['spent'] = df['광고세트'].astype(str).str.strip().map(cost_map.to_dict()).fillna(0)

    return df

# =====================
# Meta 상태 변환
# =====================
def meta_status(df):
    """광고 게재 → 상태 (active=ON, 나머지=OFF)"""
    df['상태'] = df['광고 게재'].apply(
        lambda x: 'ON' if str(x).strip().lower() == 'active' else 'OFF'
    )
    return df


# =====================
# Meta 구매 산정
# =====================
def meta_purchase(df):
    def calc(row):
        product = str(row.get('광고상품', '')).strip()
        if product in ['앱리마케팅', 'Meta APP']:
            return pd.to_numeric(row.get('구매 (AF)', 0), errors='coerce') or 0
        elif product == 'DPA':
            web = pd.to_numeric(row.get('구매(WEB)', 0), errors='coerce') or 0
            af  = pd.to_numeric(row.get('구매 (AF)', 0), errors='coerce') or 0
            return web + af
        else:
            web = pd.to_numeric(row.get('구매(WEB)', 0), errors='coerce') or 0
            app = pd.to_numeric(row.get('구매(APP)', 0), errors='coerce') or 0
            return web + app
    df['구매'] = df.apply(calc, axis=1)
    return df


# =====================
# Meta 매출액 산정
# =====================
def meta_revenue(df):
    def calc(row):
        product = str(row.get('광고상품', '')).strip()
        if product in ['앱리마케팅', 'Meta APP']:
            return pd.to_numeric(row.get('매출액 (AF)', 0), errors='coerce') or 0
        elif product == 'DPA':
            web = pd.to_numeric(row.get('매출액(WEB)', 0), errors='coerce') or 0
            af  = pd.to_numeric(row.get('매출액 (AF)', 0), errors='coerce') or 0
            return web + af
        else:
            web = pd.to_numeric(row.get('매출액(WEB)', 0), errors='coerce') or 0
            app = pd.to_numeric(row.get('매출액(APP)', 0), errors='coerce') or 0
            return web + app
    df['매출액'] = df.apply(calc, axis=1)
    return df


# =====================
# 함수 등록 딕셔너리
# (main.py에서 호출용)
# =====================
custom_functions = {
    'extract_new_codes' : extract_new_codes,
    'ga4_index'         : ga4_index,
    'af_event_count'    : af_event_count,
    'af_revenue'        : af_revenue,
    'naver_bsa_daily'   : naver_bsa_daily,
    'meta_status'       : meta_status,
    'meta_purchase'     : meta_purchase,
    'meta_revenue'      : meta_revenue,
}