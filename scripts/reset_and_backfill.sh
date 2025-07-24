#!/bin/bash

# 이 스크립트는 데이터베이스 파일을 완전히 삭제하고,
# 박스오피스와 굿즈 이벤트 데이터를 처음부터 다시 채워넣습니다.
# 주의: 이 작업은 되돌릴 수 없습니다!

# 프로젝트 루트 디렉토리로 이동 (스크립트가 어디서 실행되든 상관없이)
cd "$(dirname "$0")/.." || exit

DB_FILE="db/movie.sqlite"

echo "⚠️ 경고: 데이터베이스 파일($DB_FILE)을 삭제하고 모든 데이터를 초기화합니다."
read -p "정말로 계속하시겠습니까? (y/N): " confirm

if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "작업이 취소되었습니다."
    exit 0
fi

echo "1. 기존 데이터베이스 파일 삭제 중..."
rm -f "$DB_FILE"
echo "   => 데이터베이스 파일 삭제 완료."

echo "2. 영화 데이터 백필 실행 중..."
python src/scripts/backfill_movie.py

echo "3. 박스오피스 데이터 백필 실행 중..."
python src/scripts/backfill_boxoffice.py

echo "4. 굿즈 이벤트 데이터 백필 실행 중..."
python src/scripts/backfill_goods_events.py

echo "✅ 모든 백필 작업이 완료되었습니다."