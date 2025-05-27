# README - Running Spark Job in Docker Container

## Prerequisites

- Docker installed and running
- Spark cluster container running (`spark-master` container)

---

## Commands Used

1. **Check files inside the container:**

```bash
docker exec -it spark-master ls -l /opt/bitnami/spark/


2. **Run Spark job inside the container::**
```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --class ma.enset.App1 /opt/bitnami/spark/app.jar
