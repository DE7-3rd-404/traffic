# 프로젝트 소개
- 주제 </br>
  - 서울시 공공데이터 돌발도로정보
- 목표 </br>
  - 실시간 데이터 통합: 분산된 돌발 정보를 단일 플랫폼(Snowflake)으로 통합.
  - 데이터 품질 확보: dbt를 활용하여 데이터 정합성 및 분석 가능한 형태로 구조화.
  - 시각화 제공: Superset을 통해 현황을 실시간으로 모니터링할 수 있는 대시보드 제공.
 
# 프로젝트 구현

## 기술스택
| 구분          | 기술 스택                 | 주요 활용 목적                     |
| ----------- | --------------------- | ---------------------------- |
| 워크플로우       | Apache Airflow        | 데이터 파이프라인 자동화 및 스케줄링         |
| 데이터 웨어하우스   | Snowflake             | 대용량 데이터 통합 저장 및 고성능 분석       |
| 클라우드 스토리지   | AWS S3                | 원천 데이터(CSV 등) 안전한 저장         |
| 데이터 변환(ELT) | dbt (Data Build Tool) | Snowflake 기반 SQL 모델링 및 변환    |
| 시각화/대시보드    | Apache Superset       | 실시간 데이터 시각화 및 대시보드 구축        |
| 협업/문서/버전관리  | Slack, Notion, GitHub | 커뮤니케이션, 문서화, 이슈 관리, 코드 버전 관리 |


## 프로젝트 구조
```
[API 수집]
        ↓
(Airflow + Python)
        ↓
[S3 Raw Zone 저장]
        ↓
[Snowflake Stage / Transform]
        ↓
[Analytics Table / dbt Modeling]
        ↓
[Superset Dashboard]
```
<img width="1258" height="397" alt="프로젝트 아키텍처" src="https://github.com/user-attachments/assets/b71d631a-9ad2-4309-9c64-c6daa3747e75" />

## 시각화 화면
![project-3-team-4_superset_dashboard](https://github.com/user-attachments/assets/3ce1dfe2-0040-4812-8418-d260c4441f78)

