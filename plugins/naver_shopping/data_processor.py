import pandas as pd
import os
from datetime import datetime
import logging

class DataProcessor:
    @staticmethod
    def clean_products_data(products):
        """
        데이터 정리 (HTML 태그 제거 등)

        Args:
            products (list): 원본 상품 데이터 리스트

        Returns:
            list: 정리된 상품 데이터 리스트
        """
        cleaned_products = []

        for product in products:
            # 필요한 컬럼만 추출하고 정리
            cleaned_product = {
                'title': product.get('title', '').replace('<b>', '').replace('</b>', ''),
                'link': product.get('link', ''),
                'image': product.get('image', ''),
                'lprice': product.get('lprice', ''),
                'mallName': product.get('mallName', ''),
                'productId': product.get('productId', ''),
                'productType': product.get('productType', ''),
                'brand': product.get('brand', ''),
                'maker': product.get('maker', ''),
                'category1': product.get('category1', ''),
                'category2': product.get('category2', ''),
                'category3': product.get('category3', ''),
            }
            cleaned_products.append(cleaned_product)

        return cleaned_products

    @staticmethod
    def save_to_csv(products, filename=None):
        """
        데이터를 CSV 파일로 저장

        Args:
            products (list): 저장할 상품 데이터 리스트
            filename (str, optional): 파일명. 기본값은 타임스탬프 포함

        Returns:
            str: 저장된 파일의 전체 경로
        """

        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"naver_shopping_products_{timestamp}.csv"

        # DataFrame 생성
        df = pd.DataFrame(products)

        # CSV 저장 경로 설정
        # 1. docker-compose.yaml 파일에서 airflow-init service에서 data 폴더 추가
        # 2. docker-compose.yaml 파일에서 data 폴더 volume 추가 -> /opt/airflow/data에 저장되면 로컬의 data 폴더에도 csv 저장
        output_dir = "/opt/airflow/data"
        os.makedirs(output_dir, exist_ok=True)

        filepath = os.path.join(output_dir, filename)

        # CSV 저장 (한글 지원을 위해 utf-8-sig 인코딩 사용)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')

        logging.info(f"CSV 파일 저장 완료: {filepath}")
        logging.info(f"저장된 상품 수: {len(df)}개")

        return filepath