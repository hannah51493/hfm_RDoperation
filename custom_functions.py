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
    def build_index(row):
        src = str(row.get('소스매체', ''))
        if src in ['Apple / search', 'ig / paid']:
            return src + str(row.get('캠페인', ''))
        elif src in ['google / display-shopping']:
            return src + str(row.get('캠페인', '')) + str(row.get('검색어', ''))
        else:
            return src + str(row.get('캠페인', '')) + str(row.get('컨텐츠', ''))

    df['인덱스(ga4)'] = df.apply(build_index, axis=1)
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
        lambda x: x['Event Revenue']
        if x['Event Name'] == 'af_purchase' else 0,
        axis=1
    )
    return df

# =====================
# 네이버 브랜드검색 일할 소진금액
# =====================
def naver_bsa_daily(df, bsa_cost_df, year_month):
    """
    Config_BSAcost 시트 기준
    - 기준열 : 타겟팅
    - 월별 예산열 : YYYYMM 형식으로 우측 확장
    - year_month : '2026-03' 형식으로 입력
    """
    period     = pd.Period(year_month, 'M')
    days       = period.days_in_month
    date_range = pd.date_range(
        start=f'{year_month}-01',
        periods=days,
        freq='D'
    )

    # YYYYMM 형식으로 변환 ex) '2026-03' → '202603'
    month_col = year_month.replace('-', '')

    # 해당 월 컬럼 없으면 에러 방지
    if month_col not in bsa_cost_df.columns:
        raise ValueError(f"Config_BSAcost 시트에 {month_col} 열이 없습니다.")

    rows = []
    for _, cost_row in bsa_cost_df.iterrows():
        targeting    = cost_row['타겟팅']
        monthly_cost = cost_row[month_col]

        # 예산 없는 행 스킵
        if pd.isna(monthly_cost) or monthly_cost == 0:
            continue

        daily_cost = round(monthly_cost / days)

        for date in date_range:
            rows.append({
                'date'    : date.date(),
                '타겟팅'  : targeting,
                'spent'   : daily_cost
            })

    return pd.DataFrame(rows).reset_index(drop=True)

# =====================
# 함수 등록 딕셔너리
# (main.py에서 호출용)
# =====================
custom_functions = {
    'extract_new_codes' : extract_new_codes,
    'ga4_index'       : ga4_index,
    'af_event_count'  : af_event_count,
    'af_revenue'      : af_revenue,
    'naver_bsa_daily' : naver_bsa_daily,
}