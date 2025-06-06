# shopping_data_collector.py
import time
import logging
from datetime import datetime
import sys

sys.path.append('/opt/airflow')
from plugins.naver_shopping.naver_api_client import NaverAPIClient
from plugins.naver_shopping.data_processor import DataProcessor
from plugins.config import CATEGORY_CONFIG, API_CONFIG


class ShoppingDataCollector:
    def __init__(self):
        self.api_client = NaverAPIClient()
        self.data_processor = DataProcessor()

    def collect_all_categories(self):
        """
        모든 카테고리의 상품 데이터를 수집
        """
        all_results = []
        total_collected = 0
        category_cnt = 0

        for category, target_count in CATEGORY_CONFIG.items():
            # 카테고리별 상품 검색
            category_results = self.api_client.search_products(category, target_count)

            # 검색 카테고리 정보 추가
            for item in category_results:
                item['search_category'] = category

            all_results.extend(category_results)
            total_collected += len(category_results)
            category_cnt += 1

            # 카테고리 간 호출 간격
            time.sleep(API_CONFIG["category_delay"])

        logging.info(f"전체 수집 완료: {total_collected}개 상품")

        return {
            'products': all_results,
            'total_products': total_collected,
            'categories_processed': category_cnt
        }

    def process_and_save(self, collection_result):
        """수집된 데이터를 처리하고 저장"""
        products = collection_result['products']

        # 데이터 정리
        cleaned_products = self.data_processor.clean_products_data(products)
        logging.info(f"데이터 정리 완료: {len(cleaned_products)}개 상품")

        # CSV 파일로 저장
        csv_filepath = self.data_processor.save_to_csv(cleaned_products)

        return {
            'total_products': collection_result['total_products'],
            'categories_processed': collection_result['categories_processed'],
            'csv_file_path': csv_filepath,
            'collection_time': datetime.now().isoformat()
        }