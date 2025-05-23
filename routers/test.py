#DB_NAME: 코드 상단의 DB_NAME = "실제_데이터베이스_이름을_입력하세요" 부분을 실제 사용하시는 데이터베이스 이름으로 변경해주세요.
#TABLE_NAME: TABLE_NAME = "실제_테이블_이름을_입력하세요" 부분을 해당 데이터가 저장된 테이블 이름으로 변경해주세요.
#컬럼 이름: 코드 내 SQL 쿼리문에 사용된 컬럼 이름들(lat, lon, geometry, 행정동, 시군구)이 실제 데이터베이스 테이블의 컬럼 이름과 일치하는지 다시 한번 확인해주세요.
#          만약 다르다면 쿼리문 내의 컬럼 이름들을 수정해야 합니다. (예: SELECT \실제위도컬럼명`, ...`)
#dbmodule.py 파일이 main.py 파일과 동일한 디렉토리(프로젝트 루트)에 있고, routers 폴더가 그 하위에 있는 구조인지 확인해주세요. (from dbmodule import dbmodule 임포트 경로 때문)

# routers/test.py
from fastapi import APIRouter, HTTPException, Query
import pandas as pd
from shapely import wkt
from shapely.errors import WKTReadingError

from dbmodule import dbmodule

router = APIRouter()
db_handler = dbmodule()

DB_NAME = "실제_데이터베이스_이름을_입력하세요"
TABLE_NAME = "실제_테이블_이름을_입력하세요"


def parse_geometry_to_list(geometry_wkt: str | None) -> list:
    """
    WKT 형식의 geometry 문자열을 네이버 지도 API용 [{lat: 위도, lng: 경도}, ...] 좌표 리스트로 파싱합니다.
    (이 함수는 이전과 동일)
    """
    if not geometry_wkt:
        return []
    try:
        geom = wkt.loads(geometry_wkt)
        coords_list = []
        if geom.is_empty: return []
        if geom.geom_type == 'Polygon':
            if geom.exterior: coords_list = list(geom.exterior.coords)
        elif geom.geom_type == 'MultiPolygon':
            if geom.geoms:
                first_polygon = geom.geoms[0]
                if not first_polygon.is_empty and first_polygon.exterior:
                    coords_list = list(first_polygon.exterior.coords)
        else:
            print(f"지원하지 않는 geometry 타입입니다: {geom.geom_type}")
            return []
        return [{"lat": coord[1], "lng": coord[0]} for coord in coords_list]
    except Exception as e:
        print(f"geometry 파싱 중 오류: {e}")
        return []


# 예시 URL: GET http://localhost:8000/backend/selected_coordinates?address=부산시%20해운대구%20재송동
# (만약 main.py에서 test_router에 prefix="/api" 등이 설정되었다면, 예: http://localhost:8000/api/backend/selected_coordinates?address=...)
@router.get(
    "/backend/selected_coordinates",
    summary="주소 문자열 기반 좌표 및 경계 정보 조회 (이름/코드 비교 방식)"
)
async def get_coordinates_by_flexible_address_search(
    address: str = Query(..., min_length=1, description="검색할 주소 문자열 (예: '부산 해운대 재송', '해운대구', '재송동')")
):
    """
    입력된 주소 문자열의 부분을 데이터베이스의 '시군구' 및 '행정동' 이름과 비교하여 지역 정보를 검색합니다.
    부분 일치(예: '해운대'로 '해운대구' 검색)를 지원하며, '구'와 '동'이 함께 유추될 경우 '동' 정보를 우선합니다.

    - 예1: "부산시 해운대 재송" -> '해운대구 재송동' 정보 반환 시도
    - 예2: "부산시 해운대구" -> '해운대구' 정보 반환 시도
    - 경계 데이터(`multiPolygon`)는 `[{lat: 위도, lng: 경도}, ...]` 형식입니다.
    """
    if not address or not address.strip():
        raise HTTPException(status_code=400, detail="주소 문자열('address')을 입력해야 합니다.")

    db_connection_engine = db_handler.get_db_con(db_name=DB_NAME)
    df = pd.DataFrame()
    result_level: str = ""
    matched_name_for_log: str = f"입력값({address})에 대한 분석 시도"
    # matched_name_for_log은 DB에서 실제로 매칭된 이름을 반영하도록 아래 로직에서 업데이트 됩니다.
    # 초기값은 혹시 모를 에러 상황에서 사용될 수 있습니다.

    raw_parts = [p.strip() for p in address.strip().split() if p.strip()]

    if not raw_parts:
        raise HTTPException(status_code=400, detail=f"주소 '{address}'에서 검색어를 추출할 수 없습니다.")

    # 검색 시나리오 시작
    # 시나리오 1: 입력된 주소의 마지막 두 부분이 각각 '구 후보'와 '동 후보'일 경우
    if len(raw_parts) >= 2:
        gu_candidate = raw_parts[-2]
        dong_candidate = raw_parts[-1]
        
        query = f"""
            SELECT `lat`, `lon`, `geometry`, `시군구` as db_sigungu_name, `행정동` as db_haengjeongdong_name
            FROM `{TABLE_NAME}`
            WHERE `시군구` LIKE %(gu_cand)s AND `행정동` LIKE %(dong_cand)s
            ORDER BY LENGTH(`시군구`), LENGTH(`행정동`)
            LIMIT 1
        """
        temp_df = pd.read_sql_query(query, db_connection_engine, params={
            "gu_cand": f"{gu_candidate}%",
            "dong_cand": f"{dong_candidate}%"
        })
        if not temp_df.empty:
            df = temp_df
            result_level = "동"
            matched_name_for_log = f"{df.iloc[0]['db_sigungu_name']} {df.iloc[0]['db_haengjeongdong_name']}"

    # 시나리오 2: 위에서 못 찾았거나, 입력 부분이 1개일 경우, 마지막 부분을 '동 후보'로 검색
    if df.empty and len(raw_parts) >= 1:
        dong_candidate = raw_parts[-1]
        
        query = f"""
            SELECT `lat`, `lon`, `geometry`, `시군구` as db_sigungu_name, `행정동` as db_haengjeongdong_name
            FROM `{TABLE_NAME}`
            WHERE `행정동` LIKE %(dong_cand)s
            ORDER BY LENGTH(`행정동`)
            LIMIT 1
        """
        temp_df = pd.read_sql_query(query, db_connection_engine, params={"dong_cand": f"{dong_candidate}%"})
        if not temp_df.empty:
            df = temp_df
            result_level = "동"
            matched_name_for_log = f"{df.iloc[0]['db_sigungu_name']} {df.iloc[0]['db_haengjeongdong_name']}"
    
    # 시나리오 3: 위에서 여전히 못 찾았고, 입력 부분이 1개일 경우 (또는 그 이상이지만 위 시나리오들에서 실패), 마지막 부분을 '구 후보'로 검색
    if df.empty and len(raw_parts) >= 1:
        gu_candidate = raw_parts[-1]
        
        query = f"""
            SELECT `lat`, `lon`, `geometry`, `시군구` as db_sigungu_name, NULLIF('', '') as db_haengjeongdong_name
            FROM `{TABLE_NAME}`
            WHERE `시군구` LIKE %(gu_cand)s
            ORDER BY LENGTH(`시군구`)
            LIMIT 1
        """
        temp_df = pd.read_sql_query(query, db_connection_engine, params={"gu_cand": f"{gu_candidate}%"})
        if not temp_df.empty:
            df = temp_df
            result_level = "구"
            matched_name_for_log = df.iloc[0]['db_sigungu_name']

    if df.empty or not result_level:
        initial_search_terms = ", ".join(raw_parts)
        raise HTTPException(status_code=404, detail=f"입력 주소 '{address}' (분석된 검색어 시도: '{initial_search_terms}')에 해당하는 지역 정보를 찾을 수 없습니다.")

    data_row = df.iloc[0]
    try:
        latitude = float(data_row['lat'])
        longitude = float(data_row['lon'])
        geometry_wkt_from_db = data_row['geometry']
    except KeyError as e:
        print(f"DB 결과에 필수 컬럼 누락: {e} (검색된 지역: '{matched_name_for_log}')")
        raise HTTPException(status_code=500, detail="DB 스키마 불일치 또는 데이터 누락.")
    except (ValueError, TypeError) as e:
        print(f"DB 좌표값 형식 오류 ('{matched_name_for_log}'): {e}")
        raise HTTPException(status_code=500, detail="DB 좌표 형식 오류.")

    parsed_polygon_coords = parse_geometry_to_list(geometry_wkt_from_db)

    if not parsed_polygon_coords and geometry_wkt_from_db and geometry_wkt_from_db.strip() != "":
        print(f"Geometry WKT 파싱 실패: '{geometry_wkt_from_db}' ('{matched_name_for_log}')")
        raise HTTPException(status_code=500, detail=f"'{matched_name_for_log}'의 경계 데이터 처리 중 오류.")

    return {
        "coordinates": [longitude, latitude],
        "multiPolygon": parsed_polygon_coords,
        "matched_level": result_level,
        "matched_name": matched_name_for_log
    }
