drop table if exists finance.o2o_request_code_map_jake
;
create table finance.o2o_request_code_map_jake
with (
      format = 'parquet',
      write_compression = 'SNAPPY',
      external_location = 's3://bucketplace-emr-ba/hive-ba/output/finance/o2o_request_code_map_jake2/'
)
as (
select 'status_code' col, 0 code, '수락대기' description
union all select 'status_code' col, 15 code, '상담거절' description
union all select 'status_code' col, 14 code, '매칭완료' description
union all select 'status_code' col, 16 code, '대면예정' description
union all select 'status_code' col, 17 code, '대면완료' description
union all select 'status_code' col, 18 code, '미대면종료' description
union all select 'status_code' col, 19 code, '대면후종료' description
union all select 'status_code' col, 11 code, '계약완료' description
union all select 'status_code' col, 12 code, '계약인증' description
union all select 'status_code' col, 13 code, '계약신고' description
union all select 'status_code' col, 20 code, '계약실패' description
union all select 'status_code' col, 7 code, 'LEGACY_상담중단' description
union all select 'rejectreason_code' col, 0 code, '일정임박' description
union all select 'rejectreason_code' col, 1 code, '상담인원부족' description
union all select 'rejectreason_code' col, 2 code, '시공인원부족' description
union all select 'rejectreason_code' col, 3 code, '최소예산미달' description
union all select 'rejectreason_code' col, 4 code, '고객예산부족' description
union all select 'rejectreason_code' col, 5 code, '시공불가지역' description
union all select 'rejectreason_code' col, 6 code, '주력분야아님' description
union all select 'rejectreason_code' col, 7 code, '자동거절' description
union all select 'rejectreason_code' col, 8 code, '너무먼일정' description
union all select 'rejectreason_code' col, 99 code, '직접입력' description
union all select 'rejectreason_code' col, 999 code, 'UNKNOWN' description
union all select 'cancellationreason_code' col, 0 code, '일정임박' description
union all select 'cancellationreason_code' col, 1 code, '상담인원_부족' description
union all select 'cancellationreason_code' col, 2 code, '시공인원_부족' description
union all select 'cancellationreason_code' col, 3 code, '최소예산기준_미달' description
union all select 'cancellationreason_code' col, 4 code, '고객예산_부족' description
union all select 'cancellationreason_code' col, 5 code, '시공불가지역' description
union all select 'cancellationreason_code' col, 6 code, '고객_연락불가' description
union all select 'cancellationreason_code' col, 7 code, '고객_취소요청' description
union all select 'cancellationreason_code' col, 8 code, '고객_시공의사_철회' description
union all select 'cancellationreason_code' col, 9 code, '요구사항_대응불가' description
union all select 'cancellationreason_code' col, 10 code, '타업체_계약' description
union all select 'cancellationreason_code' col, 11 code, '너무_먼_일정' description
union all select 'cancellationreason_code' col, 99 code, '직접입력' description
union all select 'cancellationreason_code' col, 999 code, 'UNKNOWN' description
union all select 'inflowchannel_code' col, 0 code, '맞춤업체추천' description
union all select 'inflowchannel_code' col, 1 code, '리스팅' description
union all select 'inflowchannel_code' col, 2 code, '시공사례' description
union all select 'inflowchannel_code' col, 3 code, '시공스토어(legacy)' description
union all select 'inflowchannel_code' col, 4 code, '자동추천(deprecated)' description
union all select 'inflowchannel_code' col, 5 code, '자동매칭(deprecated)' description
union all select 'inflowchannel_code' col, 6 code, '추가매칭(deprecated)' description
union all select 'inflowchannel_code' col, 99 code, '이벤트' description
union all select 'residencetype_code' col, 0 code, '아파트' description
union all select 'residencetype_code' col, 1 code, '오피스텔' description
union all select 'residencetype_code' col, 2 code, '빌라' description
union all select 'residencetype_code' col, 3 code, '단독주택' description
union all select 'residencetype_code' col, 4 code, '사무공간' description
union all select 'residencetype_code' col, 5 code, '상업공간' description
union all select 'residencetype_code' col, 6 code, '외식_상업공간' description
union all select 'residencetype_code' col, 7 code, '매장_상업공간' description
union all select 'residencetype_code' col, 8 code, '오피스_상업공간' description
union all select 'residencetype_code' col, 9 code, '교육_상업공간' description
union all select 'residencetype_code' col, 10 code, '숙박_상업공간' description
union all select 'residencetype_code' col, 11 code, '기타_상업공간' description
union all select 'hopedschedule_code' col, 10 code, '1주일 이내' description
union all select 'hopedschedule_code' col, 11 code, '1주~2주 이내' description
union all select 'hopedschedule_code' col, 12 code, '2주~1달 이내' description
union all select 'hopedschedule_code' col, 13 code, '3주~1달 이내' description
union all select 'hopedschedule_code' col, 100 code, '1달~2달 이내' description
union all select 'hopedschedule_code' col, 101 code, '2달~3달 이내' description
union all select 'hopedschedule_code' col, 102 code, '2달 이후' description
union all select 'hopedschedule_code' col, 103 code, '3달 이후' description
union all select 'matchingstrategy_code' col, 0 code, '수동_초도매칭' description
union all select 'matchingstrategy_code' col, 1 code, '수동_추가매칭' description
union all select 'matchingstrategy_code' col, 2 code, '자동_초도매칭' description
union all select 'matchingstrategy_code' col, 3 code, '자동_추가매칭' description
union all select 'matchingstrategy_code' col, 999999 code, 'UNKNOWN' description
union all select 'scale' col, 0 col, '종합' description
union all select 'scale' col, 1 col, '개별' description
union all select 'scale' col, 2 col, '상업/카페' description
union all select 'expertises' col, 5 code, '종합' description
union all select 'expertises' col, 6 code, '도배' description
union all select 'expertises' col, 7 code, '욕실' description
union all select 'expertises' col, 8 code, '페인트' description
union all select 'expertises' col, 9 code, '마루' description
union all select 'expertises' col, 10 code, '도어' description
union all select 'expertises' col, 11 code, '조명' description
union all select 'expertises' col, 12 code, '주방' description
union all select 'expertises' col, 13 code, '전문디자인' description
union all select 'expertises' col, 14 code, '방산시장' description
union all select 'expertises' col, 15 code, '기타' description
union all select 'expertises' col, 16 code, '장판' description
union all select 'expertises' col, 17 code, '목공' description
union all select 'expertises' col, 18 code, '타일' description
union all select 'expertises' col, 19 code, '시트필름' description
union all select 'expertises' col, 20 code, '샷시' description
union all select 'expertises' col, 21 code, '블라인드' description
union all select 'expertises' col, 22 code, '설비' description
union all select 'expertises' col, 23 code, '바닥재' description
union all select 'expertises' col, 24 code, '발코니확장' description
union all select 'expertises' col, 25 code, '전기조명' description
union all select 'expertises' col, 26 code, '상업/카페' description
union all select 'rating' col, 0 code, '준회원1' description
union all select 'rating' col, 1 code, '준회원2' description
union all select 'rating' col, 2 code, '정회원' description
union all select 'rating' col, 3 code, '인증회원' description
union all select 'rating' col, 4 code, '멤버십' description
union all select 'rating' col, 99 code, 'UNKNOWN' description
-- 신청 종료사유 mr.closingreason
union all select 'closingreason' col, 0 code,  '허수_대면_실측불가' description
union all select 'closingreason' col, 1 code,  '허수_가견적요청' description
union all select 'closingreason' col, 2 code,  '허수_부동산계약_전' description
union all select 'closingreason' col, 3 code,  '취소_오신청' description
union all select 'closingreason' col, 4 code,  '취소_시공미정_취소' description
union all select 'closingreason' col, 5 code,  '취소_타업체선정' description
union all select 'closingreason' col, 6 code,  '취소_사유미확인' description
union all select 'closingreason' col, 7 code,  '일정_공사기간_불충분' description
union all select 'closingreason' col, 8 code,  '일정_보관이사불가' description
union all select 'closingreason' col, 9 code,  '일정_너무먼일정' description
union all select 'closingreason' col, 10 code,  '매칭가능업체없음' description
union all select 'closingreason' col, 11 code,  '시공불가지역' description
union all select 'closingreason' col, 12 code,  '연락처_오류' description
union all select 'closingreason' col, 13 code,  '비시공영역' description
union all select 'closingreason' col, 14 code,  '연락부재' description
union all select 'closingreason' col, 15 code,  '중복_DB' description
union all select 'closingreason' col, 16 code,  '테스트' description
union all select 'closingreason' col, 17 code,  '기타' description
-- 공실여부 mr.residenceinfo.residencestatus
union all select 'residencestatus' col, 2001 code, '현재공실' description
union all select 'residencestatus' col, 2002 code, '시공 시 공실예정' description
union all select 'residencestatus' col, 2003 code, '거주중(부분시공)' description
union all select 'residencestatus' col, 2004 code, '거주중(보관이사)' description
-- mr.status status_code,
union all select 'mr_status' col, 0 code, '매칭대기 (초기상태)' description
union all select 'mr_status' col, 1 code, '매칭완료 (1곳 이상 매칭 시킨 경우)' description
union all select 'mr_status' col, 2 code, '매칭종료 (어드민 상에서 매칭종료 버튼 클릭 - 종료사유필요)' description
union all select 'mr_status' col, 3 code, '매칭실패 (모든 업체에서 상담 거절)' description
union all select 'mr_status' col, 9999999 code, '시스템오류' description
-- mr.inflowchannel
union all select 'inflowchannel' col, 0 code, '간편' description
union all select 'inflowchannel' col, 1 code, '동시신청' description
union all select 'inflowchannel' col, 2 code, '알림톡' description
union all select 'inflowchannel' col, 95 code, '이벤트' description
union all select 'inflowchannel' col, 96 code, '이벤트' description
union all select 'inflowchannel' col, 97 code, '이벤트' description
union all select 'inflowchannel' col, 98 code, '이벤트' description
union all select 'inflowchannel' col, 99 code, '이벤트' description
union all select 'inflowchannel' col, 5 code, 'legacy-5' description
-- o2o impression
union all select 'o2o_impression' col, 1 code, '시공 우수업체리스트 카드' description
union all select 'o2o_impression' col, 2 code, '시공 전문가리스트 카드' description
-- o2o clean channel
union all select 'clean_channel' col, 1 code, 'O2O홈(직접)' description
union all select 'clean_channel' col, 2 code, '이사' description
union all select 'clean_channel' col, 3 code, '입주공구' description
)
;
-- insert into finance.o2o_request_code_map_jake
