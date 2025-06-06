from airflow.sdk import Variable
import logging
import requests
import time
import sys

sys.path.append('/opt/airflow')
from plugins.config import API_CONFIG

class NaverAPIClient:
    def __init__(self):
        self.headers = {
            "X-Naver-Client-Id": Variable.get("NAVER_CLIENT_ID"),
            "X-Naver-Client-Secret": Variable.get("NAVER_CLIENT_SECRET")
        }
        self.base_url = API_CONFIG["base_url"]

    def search_products(self, category, target_count):
        """
        특정 카테고리의 상품을 검색
        """
        logging.info(f"카테고리 '{category}' 상품 수집 시작 (목표: {target_count}개)")

        results = []
        start = 1
        display = min(API_CONFIG["max_display"], target_count)
        # display는 한 번에 가져올 상품 수로 최대 100개이며, target_count보다 작으면 그 수만큼 설정됨.
        # start는 검색 시작 위치로, 반복마다 display만큼 증가해 다음 페이지를 요청함.
        # results에 누적된 상품 수가 target_count에 도달할 때까지 반복 호출하여 원하는 개수만큼 수집함.

        while len(results) < target_count:
            params = {
                "query": category,
                "display": display,
                "start": start,
                "sort": "sim"
            }

            try:
                response = requests.get(self.base_url, headers=self.headers, params=params)
                response.raise_for_status()
                data = response.json()
                items = data.get("items", [])

                if not items:
                    logging.warning(f"카테고리 '{category}'에서 더 이상 상품을 찾을 수 없습니다.")
                    break

                results.extend(items)

                if len(results) >= target_count:
                    results = results[:target_count]  # 정확한 개수로 자르기
                    break

                start += display
                time.sleep(API_CONFIG["request_delay"])

            except requests.exceptions.RequestException as e:
                logging.error(f"API 호출 오류 (카테고리: {category}): {str(e)}")
                break
            except Exception as e:
                logging.error(f"예상치 못한 오류 (카테고리: {category}): {str(e)}")
                break

        collected_count = len(results)
        logging.info(f"카테고리 '{category}' 완료: {collected_count}개 상품 수집")

        return results


