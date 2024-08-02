### start 2 clusters using 
tidb-benchmark instructions
```bash
docker compose --profiles second up -d
```

### Adding some data to both clusters
```bash
mysql -h 127.0.0.1 -P 4000 -u root -e "CREATE DATABASE t1";
```

```bash
mysql -h 127.0.0.1 -P 4002 -u root -e "CREATE DATABASE t2";
```

build server docker image
```bash
docker build -t tidb-local -f Dockerfile .
```


### connecting to a cluster
```bash
docker run -it -d --net docker-setup_tidb-network-1 -v $(pwd)/config.toml:/config.toml:ro -p 4001:4001 --name tidb-local tidb-local --config config.toml 
```


### connecting to a new mysql
```bash
mysql -h 127.0.0.1 -P 4001 -u root -e "SHOW DATABASES";
+--------------------+
| Database           |
+--------------------+
| INFORMATION_SCHEMA |
| METRICS_SCHEMA     |
| PERFORMANCE_SCHEMA |
| mysql              |
| t1                 |
| t2                 |
| test               |
+--------------------+
```

load a little data to t1 and t2
```bash
sysbench --mysql-port=4000 --mysql-user=root --mysql-host=127.0.0.1 --mysql-db=t1 --threads=16 oltp_common prepare --tables=2 --table-size=500000

sysbench --mysql-port=4002 --mysql-user=root --mysql-host=127.0.0.1 --mysql-db=t2 --threads=16 oltp_common prepare --tables=2 --table-size=500000
```

run small load test on t1
```bash
db_user=root db_pwd='' db_port=4000 db_host=127.0.0.1 db_name=t1 tables=2 ./k6 run grafana-k6/point-select.js --duration 20s --vus 2
```

run small load test on t2
```bash
db_user=root db_pwd='' db_port=4002 db_host=127.0.0.1 db_name=t2 tables=2 ./k6 run grafana-k6/point-select.js --duration 20s --vus 2
```

we should be able to run same test on 4001.
```bash
db_user=root db_pwd='' db_port=4001 db_host=127.0.0.1 db_name=t1 tables=2 ./k6 run grafana-k6/point-select.js --duration 20s --vus 2
db_user=root db_pwd='' db_port=4001 db_host=127.0.0.1 db_name=t2 tables=2 ./k6 run grafana-k6/point-select.js --duration 20s --vus 2
```

# known problems:
[] background refresh doesn't update DB2 table list. requires restart.  so If sysbench data load happened after tidb-local started. it would not know about t2 tables.
