import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import re
from fake_useragent import UserAgent
import abc
from typing import List, Dict, TypedDict, Optional, Union
import logging
import html
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- 통합 데이터 구조 정의 ---

class UnifiedEvent(TypedDict):
    """통합된 이벤트 정보 구조"""
    theater_chain: str
    event_title: str
    movie_title: Optional[str]
    goods_name: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    event_url: str
    image_url: Optional[str]
    # 내부 ID
    event_id: str
    goods_id: Optional[str]
    spmtl_no: Optional[str]


class UnifiedStock(TypedDict):
    """통합된 재고 정보 구조"""
    theater_chain: str
    theater_name: str
    status: str  # 예: '보유', '소진', '소진 임박', '준비중'
    quantity: Optional[Union[int, str]]
    total_given_quantity: Optional[int]


# --- 추상 베이스 클래스 ---

class TheaterEventScraper(abc.ABC):
    """영화관 이벤트 스크레이퍼의 추상 베이스 클래스"""

    def __init__(self, chain_name: str):
        self.chain_name = chain_name
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': UserAgent().random})
        self.logger = logging.getLogger(self.__class__.__name__)

    @abc.abstractmethod
    def get_events(self) -> List[UnifiedEvent]:
        """
        진행 중인 굿즈 관련 이벤트를 크롤링하여 통합된 형식의 리스트로 반환합니다.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_goods_stock(self, event: UnifiedEvent) -> List[UnifiedStock]:
        """
        특정 이벤트에 해당하는 굿즈의 지점별 재고를 크롤링합니다.
        """
        raise NotImplementedError


# --- 영화관별 구현 클래스 ---

class CGVScraper(TheaterEventScraper):
    """CGV 이벤트 및 재고 스크레이퍼"""

    def __init__(self):
        super().__init__("CGV")
        self.EVENT_LIST_URL = "https://event-mobile.cgv.co.kr/evt/saprm/saprm/searchSaprmEvtListForPage"
        self.EVENT_DETAIL_URL = "https://event-mobile.cgv.co.kr/evt/saprm/saprm/searchSaprmEvtProdList"
        self.THEATER_STOCK_URL = "https://event-mobile.cgv.co.kr/evt/saprm/saprm/searchSaprmEvtTgtsiteList"
        self.session.headers.update({
            "Referer": "https://cgv.co.kr/",
            "Origin": "https://cgv.co.kr",
        })

    def _get_goods_info(self, event_idx: str) -> Optional[Dict[str, str]]:
        """이벤트 ID로 굿즈 정보(ID, 이름)를 조회합니다."""
        try:
            params = {"coCd": "A420", "saprmEvntNo": event_idx}
            response = self.session.get(self.EVENT_DETAIL_URL, params=params)
            response.raise_for_status()
            data = response.json()
            items = data.get("data", [])
            if items and isinstance(items, list) and len(items) > 0:
                return {
                    "id": items[0].get("spmtlProdNo"),
                    "name": items[0].get("spmtlProdNm"),
                    "spmtlNo": items[0].get("spmtlNo")
                }
        except requests.RequestException as e:
            self.logger.error(f"굿즈 정보 조회 실패 (Event ID: {event_idx}): {e}")
        except (json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"굿즈 정보 파싱 실패 (Event ID: {event_idx}): {e}")
        return None

    def get_events(self) -> List[UnifiedEvent]:
        self.logger.info("이벤트 목록 조회를 시작합니다.")
        all_events = []
        start_row = 0
        list_count = 20
        total_count = -1

        while True:
            if total_count != -1 and start_row >= total_count:
                break

            params = {
                "coCd": "A420",
                "siteNo": "",
                "startRow": start_row,
                "listCount": list_count
            }
            try:
                response = self.session.get(self.EVENT_LIST_URL, params=params)
                response.raise_for_status()
                response_data = response.json()
                data = response_data.get("data", {})

                if total_count == -1:
                    total_count = data.get("totalCount", 0)

                event_list = data.get("list", [])

                if not event_list:
                    break

                for event in event_list:
                    try:
                        event_idx = event.get("saprmEvntNo")
                        if not event_idx:
                            continue

                        event_name = event.get("evntOnlnExpoNm") or event.get("saprmEvntNm")
                        goods_info = self._get_goods_info(event_idx)
                        
                        goods_id = None
                        goods_name = None
                        spmtl_no = None
                        if goods_info:
                            goods_id = goods_info.get("id")
                            goods_name = goods_info.get("name")
                            spmtl_no = goods_info.get("spmtlNo")

                        movie_title = None
                        if event_name:
                            match = re.search(r'[<\[](.*?)[>\]]', event_name)
                            if match:
                                movie_title = match.group(1).strip()
                            goods_name = re.sub(r'\[.*?\]', '', event_name).strip()

                        start_date_str = event.get("evntStartYmd")
                        start_date = f"{start_date_str[:4]}.{start_date_str[4:6]}.{start_date_str[6:]}" if start_date_str and len(start_date_str) == 8 else start_date_str                                                                    
                        end_date_str = event.get("evntEndYmd")                                                                          
                        end_date = f"{end_date_str[:4]}.{end_date_str[4:6]}.{end_date_str[6:]}" if end_date_str and len(end_date_str) == 8 else end_date_str                     
                        
                        all_events.append(UnifiedEvent(
                            theater_chain=self.chain_name,
                            event_title=event_name,
                            movie_title=movie_title,
                            goods_name=goods_name,
                            start_date=start_date,
                            end_date=end_date,
                            event_url=f"https://cgv.co.kr/evt/giveawayStateDetail?saprmEvntNo={event_idx}",
                            image_url=event.get("attchFilePathNm"),
                            event_id=event_idx,
                            goods_id=goods_id,
                            spmtl_no=spmtl_no
                        ))
                    except (AttributeError, KeyError) as e:
                        self.logger.warning(f"개별 이벤트 파싱 중 오류 발생: {event}, 오류: {e}")
                        continue
                
                start_row += len(event_list)
                if len(event_list) < list_count:
                    break

            except requests.RequestException as e:
                self.logger.error(f"이벤트 목록 요청 실패: {e}")
                break
            except (json.JSONDecodeError, KeyError) as e:
                self.logger.error(f"이벤트 목록 파싱 실패: {e}")
                break
        
        self.logger.info(f"총 {len(all_events)}개의 이벤트를 조회했습니다.")
        return all_events

    def get_goods_stock(self, event: UnifiedEvent) -> List[UnifiedStock]:
        event_id = event.get("event_id")
        goods_id = event.get("goods_id")
        spmtl_no = event.get("spmtl_no")

        if not event_id or not goods_id or not spmtl_no:
            self.logger.warning(f"재고 조회를 위한 event_id, goods_id, 또는 spmtl_no가 없습니다: event_id={event_id}, goods_id={goods_id}, spmtl_no={spmtl_no}")
            return []

        self.logger.info(f"'{event.get('goods_name', 'N/A')}' 재고 조회를 시작합니다. (Event ID: {event_id}, Goods ID: {goods_id}, Spmtl No: {spmtl_no})")
        all_stocks = []
        
        params = {
            "coCd": "A420",
            "saprmEvntNo": event_id,
            "saprmEvntProdNo": goods_id,
            "spmtlNo": spmtl_no
        }
        
        try:
            response = self.session.get(self.THEATER_STOCK_URL, params=params)
            response.raise_for_status()
            data = response.json().get("data", [])
            theaters = data if isinstance(data, list) else data.get("list", [])

            for theater in theaters:
                rl_invnt_qty = theater.get("rlInvntQty")
                tot_pay_qty = theater.get("totPayQty")
                fcfs_pay_yn = theater.get("fcfsPayYn")

                status_standard = "알 수 없음"
                quantity = None

                if rl_invnt_qty is not None:
                    if rl_invnt_qty > 40:
                        status_standard = "보유"
                    elif rl_invnt_qty > 10:
                        status_standard = "소진중"
                    elif rl_invnt_qty > 0:
                        status_standard = "소량보유"
                    else: # rl_invnt_qty <= 0
                        status_standard = "소진"
                    quantity = rl_invnt_qty
                
                all_stocks.append(UnifiedStock(
                    theater_chain=self.chain_name,
                    theater_name=theater.get("siteNm", "알 수 없음"),
                    status=status_standard,
                    quantity=quantity,
                    total_given_quantity=tot_pay_qty
                ))
        except requests.RequestException as e:
            self.logger.error(f"재고 조회 요청 실패: {e}")
        except (json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"재고 조회 파싱 실패: {e}")
            
        return all_stocks


class LotteCinemaScraper(TheaterEventScraper):
    """롯데시네마 이벤트 및 재고 스크레이퍼"""

    def __init__(self):
        super().__init__("롯데시네마")
        self.BASE_URL = "https://www.lottecinema.co.kr/LCWS/Event/EventData.aspx"
        self.session.headers.update({
            "Referer": "https://www.lottecinema.co.kr/NLCHS/Event",
            "Origin": "https://www.lottecinema.co.kr",
        })

    def _make_request(self, method_name: str, params: Dict) -> Optional[Dict]:
        """롯데시네마 API 요청을 처리하는 헬퍼 함수"""
        payload = {
            "MethodName": method_name,
            "channelType": "HO", "osType": "W",
            "osVersion": self.session.headers['User-Agent'],
        }
        payload.update(params)
        
        files = {"paramList": (None, json.dumps(payload), "application/json")}
        
        try:
            # 세션 유지를 위해 이벤트 페이지를 먼저 방문
            self.session.get("https://www.lottecinema.co.kr/NLCHS/Event")
            response = self.session.post(self.BASE_URL, files=files)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"API 요청 실패 ({method_name}): {e}")
        except json.JSONDecodeError as e:
            self.logger.error(f"API 응답 파싱 실패 ({method_name}): {e}")
        return None

    def get_events(self) -> List[UnifiedEvent]:
        self.logger.info("이벤트 목록 조회를 시작합니다.")
        params = {
            "EventClassificationCode": "20", "SearchText": "", "CinemaID": "",
            "PageNo": 1, "PageSize": 100, "MemberNo": "0"
        }
        data = self._make_request("GetEventLists", params)
        if not data or "Items" not in data:
            return []

        all_events = []
        for item in data["Items"]:
            try:
                event_id = item.get("EventID")
                event_name = item.get("EventName", "")
                
                # 굿즈 정보 조회
                detail_data = self._make_request("GetInfomationDeliveryEventDetail", {"EventID": event_id})
                goods_items = detail_data.get("InfomationDeliveryEventDetail", [{}])[0].get("GoodsGiftItems", [])
                
                if not goods_items:
                    continue
                
                goods_info = goods_items[0]
                goods_id = goods_info.get("FrGiftID")
                goods_full_name = goods_info.get("FrGiftNm", "")

                # 영화 제목 및 굿즈 이름 파싱
                movie_title_match = re.search(r'<([^<>]+)>', event_name)
                movie_title = movie_title_match.group(1).strip() if movie_title_match else None

                if '시그니처 아트카드' in event_name:
                    goods_name = '시그니처 아트카드'
                elif 'SPECIAL ART CARD' in event_name:
                    goods_name = '스페셜 아트카드'
                else:
                    # <...> 제거
                    cleaned_goods_name = re.sub(r'<[^<>]+>', '', goods_full_name).strip()
                    # 콤마와 괄호 사이의 내용 추출
                    goods_match = re.search(r',\s*(.*?)\s*\)', cleaned_goods_name)
                    goods_name = goods_match.group(1).strip() if goods_match else cleaned_goods_name

                all_events.append(UnifiedEvent(
                    theater_chain=self.chain_name,
                    event_title=event_name,
                    movie_title=movie_title,
                    goods_name=goods_name,
                    start_date=item.get("ProgressStartDate"),
                    end_date=item.get("ProgressEndDate"),
                    event_url=f"https://www.lottecinema.co.kr/NLCHS/Event/EventTemplateInfo?eventId={event_id}",
                    image_url=item.get("ImageUrl"),
                    event_id=event_id,
                    goods_id=goods_id
                ))
            except (AttributeError, KeyError, IndexError) as e:
                self.logger.warning(f"개별 이벤트 파싱 중 오류 발생: {item}, 오류: {e}")
                continue
        
        self.logger.info(f"총 {len(all_events)}개의 이벤트를 조회했습니다.")
        return all_events

    def get_goods_stock(self, event: UnifiedEvent) -> List[UnifiedStock]:
        event_id = event.get("event_id")
        goods_id = event.get("goods_id")
        if not event_id or not goods_id:
            self.logger.warning("재고 조회를 위한 event_id 또는 goods_id가 없습니다.")
            return []

        self.logger.info(f"'{event['goods_name']}' 재고 조회를 시작합니다. (Event ID: {event_id}, Goods ID: {goods_id})")
        params = {"EventID": event_id, "GiftID": goods_id}
        data = self._make_request("GetCinemaGoods", params)
        if not data or "CinemaDivisionGoods" not in data:
            return []

        all_stocks = []
        for theater in data["CinemaDivisionGoods"]:
            quantity_str = theater.get("Cnt", "0")
            try:
                quantity = int(quantity_str)
            except (ValueError, TypeError):
                quantity = 0

            status = "알 수 없음"
            if quantity > 40:
                status = "보유"
            elif quantity > 10:
                status = "소진중"
            elif quantity > 0:
                status = "소량보유"
            else:
                status = "소진"
            
            all_stocks.append(UnifiedStock(
                theater_chain=self.chain_name,
                theater_name=theater.get("CinemaNameKR", "알 수 없음"),
                status=status,
                quantity=quantity
            ))
        return all_stocks


class MegaboxScraper(TheaterEventScraper):
    """메가박스 이벤트 및 재고 스크레이퍼"""

    def __init__(self):
        super().__init__("메가박스")
        self.EVENT_LIST_URL = "https://www.megabox.co.kr/on/oh/ohe/Event/eventMngDiv.do"
        self.EVENT_DETAIL_URL = "https://www.megabox.co.kr/event/detail"
        self.THEATER_STOCK_URL = "https://www.megabox.co.kr/on/oh/ohe/Event/selectGoodsStockPrco.do"
        self.session.headers.update({
            "Referer": "https://www.megabox.co.kr/event/movie",
            "Origin": "https://www.megabox.co.kr",
            "X-Requested-With": "XMLHttpRequest"
        })

    def _get_goods_info_from_detail(self, event_no: str) -> Optional[Dict[str, str]]:
        """이벤트 상세 페이지에서 굿즈 정보(이름, 번호)를 조회합니다."""
        try:
            response = self.session.get(self.EVENT_DETAIL_URL, params={"eventNo": event_no})
            response.raise_for_status()
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            button = soup.find("button", id="btnSelectGoodsStock")
            if button:
                goods_data = {"name": button.get("data-nm"), "id": button.get("data-pn")}
                return goods_data
            else:
                return None
        except requests.RequestException as e:
            self.logger.error(f"굿즈 정보 조회 실패 (Event No: {event_no}): {e}")
        return None

    def get_events(self) -> List[UnifiedEvent]:
        self.logger.info("이벤트 목록 조회를 시작합니다.")
        body = {
            "currentPage": "1", "recordCountPerPage": "1000", "eventStatCd": "ONG",
            "eventTitle": "", "eventDivCd": "CED03", "eventTyCd": "", "orderReqCd": "ONGlist"
        }
        try:            
            self.session.get("https://www.megabox.co.kr/event/movie")
            response = self.session.post(self.EVENT_LIST_URL, data=body)
            response.raise_for_status()
            response.encoding = response.apparent_encoding  # 인코딩 자동 감지
            soup = BeautifulSoup(response.text, 'html.parser')
            event_tags = soup.select("div.event-list > div.item")
        except requests.RequestException as e:
            self.logger.error(f"이벤트 목록 요청 실패: {e}")
            return []
        except (json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"이벤트 목록 파싱 실패: {e}")
            return []

        all_events = []
        for i, tag in enumerate(event_tags):
            try:
                link = tag.find("a")
                if not link: continue
                 # onclick 속성에서 event_no 추출
                onclick_attr = link.get("onclick", "")
                match = re.search(r"fn_eventDetail\('(\d+)'", onclick_attr)
                if not match:
                    continue
                event_no = match.group(1)
                img_tag = link.find("img")
                if not img_tag: continue

                # 원본 alt 텍스트 (HTML entity 포함)
                raw_goods_name = img_tag.get("alt", "").strip()
                event_title = raw_goods_name  # 백업용 저장
                image_url = img_tag.get("data-src")

                period_tag = link.find("p", class_="date")
                period = period_tag.get_text(strip=True) if period_tag else ""

                goods_info = self._get_goods_info_from_detail(event_no)
                if not goods_info:
                    continue

                goods_name = html.unescape(goods_info["name"]).strip()
                goods_id = goods_info["id"]

                # 영화명 추출
                movie_title = None
                unescaped_goods_name = html.unescape(raw_goods_name)
                movie_match_goods = re.search(r'[<\[](.*?)[>\]]', unescaped_goods_name)
                if movie_match_goods:
                    movie_title = movie_match_goods.group(1).strip()

                if not movie_title:
                    unescaped_event_title = html.unescape(event_title)
                    movie_match_event = re.search(r'[<\[](.*?)[>\]]', unescaped_event_title)
                    if movie_match_event:
                        movie_title = movie_match_event.group(1).strip()

                # 굿즈명에서 영화명 제거
                if movie_title:
                    goods_name = re.sub(r'\s*[<\[].*?[>\]]\s*', '', unescaped_goods_name).strip()
                    if not goods_name:
                        goods_name = re.sub(r'\s*[<\[].*?[>\]]\s*', '', unescaped_event_title).strip()
                else:
                    goods_name = unescaped_goods_name

                # 날짜 파싱
                dates = [d.strip() for d in period.split('~')]
                start_date = dates[0] if len(dates) > 0 else None
                end_date = dates[1] if len(dates) > 1 else None

                all_events.append(UnifiedEvent(
                    theater_chain=self.chain_name,
                    event_title=event_title,
                    movie_title=movie_title,
                    goods_name=goods_name,
                    start_date=start_date,
                    end_date=end_date,
                    event_url=f"{self.EVENT_DETAIL_URL}?eventNo={event_no}",
                    image_url=image_url,
                    event_id=event_no,
                    goods_id=goods_id
                ))            
            except (AttributeError, KeyError) as e:
                self.logger.warning(f"개별 이벤트 파싱 중 오류 발생: {tag}, 오류: {e}")
                continue
        
        self.logger.info(f"총 {len(all_events)}개의 이벤트를 조회했습니다.")
        return all_events


    def get_goods_stock(self, event: UnifiedEvent) -> List[UnifiedStock]:
        goods_id = event.get("goods_id")
        if not goods_id:
            self.logger.warning("재고 조회를 위한 goods_id가 없습니다.")
            return []

        self.logger.info(f"'{event['goods_name']}' 재고 조회를 시작합니다. (Goods ID: {goods_id})")
        
        all_stocks = []
        body = {"goodsNo": goods_id}
        
        try:
            response = self.session.post(self.THEATER_STOCK_URL, data=body)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            theater_tags = soup.find_all("li", class_="brch")

            for tag in theater_tags:
                theater_name = tag.find("a").get_text(strip=True) if tag.find("a") else "알 수 없는 지점"
                status = tag.find("span").get_text(strip=True) if tag.find("span") else "알 수 없음"
                all_stocks.append(UnifiedStock(
                    theater_chain=self.chain_name,
                    theater_name=theater_name,
                    status=status,
                    quantity=None
                ))
        except requests.RequestException as e:
            self.logger.error(f"재고 조회 요청 실패: {e}")
        except Exception as e:
            self.logger.error(f"재고 조회 파싱 실패: {e}")
            
        return all_stocks