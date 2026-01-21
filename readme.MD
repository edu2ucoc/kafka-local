# 설치
## 카프카
```
    docker compose up -d
```
## 파이썬 패키지 설치
```
    pip install kafka-python
```

# 기본 구성
/
L docker-compose.yaml
L bakery-producer.py
L bakery-consumer.py

# 카프카 내에서 CLI 확인
- docker상 컨테이너 내부의 카프카 위치
- cd /opt/kafka/bin
- ls
    - 각종 쉘 프로그램 확인 가능함
```
# 토픽 생성
kafka-topic.sh --create --topic 토픽명 --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1
# 파티션 1개, 복제본 1개로 준비되는 토픽이다

# 토픽 전송
kafka-console-producer.sh --topic bk-orders --bootstrap-server 127.0.0.1:9092
>> 프럼프트 열림
>> 메세지 입력 (엔터)

# 해당 토픽으로 전달되는 메세지 확인
kafka-console-consumer.sh --topic bk-orders --bootstrap-server 127.0.0.1:9092


```

# 향후 파이프라인
- 센서|웹|IOT/로그,데이터 발생 -> kafka producer 전송 -> kafka 서버 적제
    - kafka 서버 -> kafka consumer 수신
    - kafka 서버 -> kafka consumer 수신 -> s3 : 수신후 업로드 -> 약간의 지연 발생
    - kafka 서버 -> kafka connect -> s3 : 즉시 업로드 -> 패턴 설계 (10개가 쌓이면 업로드등)
    - kafka 서버 -> Logstash(ELK활용) -> s3 : 즉시 업로드 -> 패턴 설계 (10개가 쌓이면 업로드등)
                                     -> 전처리(ETL) -> opensearch
                                     -> xxxx
                                     -> xxxx
    - kafka 서버 -> Fluent bit(EFK활용) -> s3
                                       -> ...
    - kafka 서버 -> Spark Structured Streamming(대규모 복잡한 가공처리,전처리) -> s3
        - 단순하게 적제하는 것이면 오버스펙임
        - kafka 서버 -> Spark Streamming (AWS EMR) -> s3


# kafka > kafka connect > s3 : xx 데이터 실시간 업로드
## 특징
- kafka producer가 전송하는 데이터를 별도의 코드 없이 설정으로 s3에 업로드
- 설정에서 패턴 정의
- 데이터는 페이크데이터를 활용 (패키지 faker를 사용)
    ```
    pip install Faker
    ```
- 로그 발생, 전송등 포지션상 airflow 내부에 포함 x (별도 프로젝트로 진행)
- 데이터 - 웹로그 - json 포멧
    ```
        ip, timestamp, method, url, status_code, user_agent
    ```

## 데이터 파이프라인
- 로그 발생 -> kafka producer 메세지 전송 -> kafka 서버 도달 - kafka connect -> s3
    - docker-compose.yaml 서비스 추가 필요
        - kafka connect 추가 필요
    - 패턴 정의 -> 설정 파일 -> s3_conn_config.json (이름은 커스텀)
        ```
            ...
            "s3.region": "ap-northeast-2",       # 본인 리전으로 수정
            "s3.bucket.name": "airflow-ai-en-0", # 본인 버킷으로 수정
            ...
        ```
        - 데이터가 10개가 모이면 s3에 업로드 (버퍼링 10개로 잡음, 컨셉)
        ```
            "flush.size": "10",
        ```
        - 로그가 10개가 아주 늦게 모이거나, 도달하지 않으면 전송이 않되는 상황을 고려하여 일정 시간 지나면 자동 업로드(인터벌)        
            ```
                # 현재 s3_conn_config.json 파일에는 생략되어 있음
                rotate.schedule.interval.ms: 시간값(ms 단위)
            ```
        - 버킷준비 (본인 계정)
            - 버킷 패스 패턴
            ```
                "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
                버킷/토픽/토픽명/2026/01/20/10/....
            ```
    - 패턴 등록 -> 커넥션 정보 등록
        - kafka ui > kafka connect > Create new connector 
            - GUI 방식 -> 불안정함
            - 이름, 내용 등록
        - 터미널/파워쉘 등에서 직접 등록
            - 윈도우용
            ```
            curl.exe -X POST http://localhost:8083/connectors/ -H "Content-Type: application/json" -d "@s3_conn_config.json"
            
            or

            Invoke-RestMethod -Uri "http://localhost:8083/connectors/" -Method Post -ContentType "application/json" -InFile "s3_conn_config.json"
            ```
            - 맥용
            ```
                curl -X POST http://localhost:8083/connectors/ -H "Content-Type: application/json" -d @s3_conn_config.json
            ```


- kafka connect
    - 데이터(로그(도메인별), 뉴스, ... ) 발생 => 카프카 프로듀서를 통해 여러곳에서 동시 다발적으로 카프카에 전송 => 카프카 커넥터 => S3에 자동 적제됨 (Data Lake)
    - 만약, s3 적제 하면서 다른 서비스에 추가로 저장등 작업을 원하면 -> Logstash, FLuent bit등 사용 동시에 멀티 작업에 대한 구성 필요

## kafka 서버 -> Logstash(ELK활용) -> s3  and 전처리(ETL)? -> opensearch : 동시전송
## 구성
    - Logstash에 서비스 설치
    - 패턴 구성 (동시 처리에 대한 패턴 정의)
        - 1. s3 -> raw 데이터 보관용
        - 2. opensearch -> 검색분석용
        - 3. athena -> 분석용
        - ....
    - opensearch에 저장된 데이터 -> ariflow은 스케줄을 통해서 특정 주기별로 후속작업 진행(배치 패턴)

## 구조
/
├── docker-compose.yaml   # [유지]컨테이너 구성 + [v 추가] 서비스에 logstash 추가
├── web_log_producer.py   # [유지]로그 발생기
├── .env                  # [유지]AWS 엑세스키 + [v 추가] opensearch 마스터 ID/PW
├── pipeline
|       L logstash.conf   # [v 신규] 동시 전송 규칙 구성 
├── logstash
|       L Dockerfile      # [v 신규] logstash에 대한 이미지
├── 기타 파일 유지 (*.py)

## .env 
```
AWS_ACCESS_KEY_ID=..
AWS_SECRET_ACCESS_KEY=..
# 추가분
AWS_OPENSEARCH_HOST=.......amazonaws.com
AWS_OPENSEARCH_USER=..
AWS_OPENSEARCH_PASS=..
```

## docker-compose.yaml 추가
```
서비스
...

logstash:
    # 도커 파일로 부터 빌드후 이미지를 대상으로 컨테이너 구성 -> --build (1회)
    build: 
      context: ./logstash
    container_name: logstash
    # 설정 파일(conf)을 외부에서 작성된것이 컨테이너 내부로 공유됨 -> logstash의 설정이됨
    # 여기에 s3, opensearch에 동시 업로드하는 구성이 세팅됨
    volumes:
      - ./pipeline/:/usr/share/logstash/pipeline/
    environment:
      # 자바로 구성된 프로그램 => logstash => 자비 힙 메모리 설정(운영)
      LS_JAVA_OPTS: "-Xmx512m -Xms512m"
      # AWS 접속 정보 -> .env를 알아서 읽어서 세팅함
      # S3 접속용 (엑세스키)
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      # AWS OpenSearch 접속용 환경변수 전달 (마스터 계정)
      AWS_OPENSEARCH_HOST: ${AWS_OPENSEARCH_HOST}
      AWS_OPENSEARCH_USER: ${AWS_OPENSEARCH_USER}
      AWS_OPENSEARCH_PASS: ${AWS_OPENSEARCH_PASS}
    depends_on:
      - kafka
```

## 구성
docker compose down
docker compose up -d --build


# EFK
- logstash -> Fluent-bit 교체
## 수정
/
L docker-compose.yaml    # [v 수정] logstash 삭제 -> fluent-bit 추가
L fluent-bit
    L fluent-bit.conf    # [신규] 파이프라인 구성 (INPUT, FILTER, OUTPUT)
    L parsers.conf       # [신규] JSON 파싱 설정
L logstash               # [v 삭제] logstash 폴더
L pipeline               # [v 삭제] pipeline 폴더
L ....                   # 이외 파일들은 유지

## 메세지 구조 비교
```
# 원본 메세지 
{'ip': '64.79.193.226', 'timestamp': '2026-01-21T10:06:00', 'method': 'TRACE', 'url': 'http://www.schmitt.com/app/explorehome.htm', 'status_code': 394, 'user_agent': 'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.1)'}

# Fluent-bit 수신 메세지
# 원본 메세지(데이터)는 "payload"키의 값으로 배치 됨
# 새로운 구조 : topic, partition, offset, error, key, payload=원본데이터
[0] raw.logs: [[1768957560.331852339, {}], {"topic"=>"web_logs", "partition"=>0, "offset"=>112, "error"=>nil, "key"=>nil, "payload"=>{"ip"=>"64.79.193.226", "timestamp"=>"2026-01-21T10:06:00", "method"=>"TRACE", "url"=>"http://www.schmitt.com/app/explorehome.htm", "status_code"=>394, "user_agent"=>"Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.1)"}}]
```

### payload 리프팅후 결과
- payload 내부값이 상위 레벨로 이동, payload 키값 자체를 삭제됨
```
[0] raw.logs: [[1768958290.259928134, {}], {"topic"=>"web_logs", "partition"=>0, "offset"=>117, "error"=>nil, "key"=>nil, "ip"=>"107.126.76.224", "timestamp"=>"2026-01-21T10:18:09", "method"=>"HEAD", "url"=>"http://pace-scott.com/tagsmain.html", "status_code"=>417, "user_agent"=>"Mozilla/5.0 (Windows; U; Windows NT 6.1) AppleWebKit/532.31.6 (KHTML, like Gecko) Version/4.0.1 Safari/532.31.6"}]
```

### 원본 복제후 결과
- 원본과 사본에 대해 태그만 다르게 구성
```
[0] raw.logs: [[1768958701.710106057, {}], {"topic"=>"web_logs", "partition"=>0, "offset"=>121, "error"=>nil, "key"=>nil, "ip"=>"49.93.193.158", "timestamp"=>"2026-01-21T10:25:01", "method"=>"HEAD", "url"=>"http://mcdonald.com/listcategory.html", "status_code"=>209, "user_agent"=>"Mozilla/5.0 (iPad; CPU iPad OS 10_3_3 like Mac OS X) AppleWebKit/534.0 (KHTML, like Gecko) FxiOS/16.6y3393.0 Mobile/56J972 Safari/534.0"}]

[0] processed.logs: [[1768958701.710106057, {}], {"topic"=>"web_logs", "partition"=>0, "offset"=>121, "error"=>nil, "key"=>nil, "ip"=>"49.93.193.158", "timestamp"=>"2026-01-21T10:25:01", "method"=>"HEAD", "url"=>"http://mcdonald.com/listcategory.html", "status_code"=>209, "user_agent"=>"Mozilla/5.0 (iPad; CPU iPad OS 10_3_3 like Mac OS X) AppleWebKit/534.0 (KHTML, like Gecko) FxiOS/16.6y3393.0 Mobile/56J972 Safari/534.0"}]
```