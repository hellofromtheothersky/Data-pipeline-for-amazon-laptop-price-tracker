cd /opt/airflow/amazon_data_scraper
scrapy crawl laptop_list -o laptop_list.csv --logfile log_laptop_list.log -a list_max_len=5