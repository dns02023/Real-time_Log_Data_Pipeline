# Real-time_Log_Data_Pipeline
Elasticsearch-Logstash-Kibana / Kafka / Spark Streaming 을 활용한 실시간 로그 데이터 ETL / Visualization Data Pipeline  





- Data Pipeline 구조

![data_pipeline_github](https://user-images.githubusercontent.com/20104945/91434208-7181d680-e89f-11ea-93f3-099f2642bc91.png)







- Data Pipeline Flow  


1. 맛집 추천 웹서비스에 사용자가 접속하고, (웹 서버에 관한 내용은 Restaurant_Recsys_Web_server 레포지토리를 참고해주세요.)  


![메인페이지](https://user-images.githubusercontent.com/20104945/91588177-bcc8e180-e992-11ea-9b95-d5ccdc85cf4c.png)  



2. 특정 맛집 정보들을 상세 페이지로 조회하면, 실시간으로 조회 행동 로그가 Logstash로 수집(data extract)되며, filtering과 parsing 작업을 거쳐 Kafka topic으로 전달됩니다.    


![상세페이지](https://user-images.githubusercontent.com/20104945/91588185-bfc3d200-e992-11ea-9215-20034bdfbb3f.png)



3. 이후, Spark Structured Streaming 에서 Kafka와의 연동을 통해 사용자별 로그 dataframe aggregation, 맛집별 tag 데이터 처리를 통해 stream data에 대한 data transform을 수행합니다.  
이어서 해당 stream data를 Elasticsearch에 전달하면서 data load를 수행합니다.
자세한 사항은 pyspark 소스코드를 참고해주세요!   

4. 마지막으로, ETL 작업을 거친 로그 데이터들에 대한 실시간 데이터 시각화 분석을 Kibana에서 수행합니다.   
 * 사용자가 조회한 맛집들의 tag 데이터들로 사용자의 맛집 선호도를 파악할 수 있는 tag cloud by Kibana


![user_log_tag_cloud](https://user-images.githubusercontent.com/20104945/91430638-2b764400-e89a-11ea-9140-2988b6618b5a.PNG)

