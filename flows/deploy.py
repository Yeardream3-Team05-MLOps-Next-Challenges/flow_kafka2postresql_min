import os

from prefect import flow
from prefect.deployments import DeploymentImage
from prefect.client.schemas.schedules import CronSchedule

from flow import min_kafka2postgresql_flow

if __name__ == "__main__":
    min_kafka2postgresql_flow.deploy(
        name="min-kafka2postgresql-deployment",
        work_pool_name="docker-agent-pool",
        work_queue_name="docker-agent",
        image=DeploymentImage(
            name="min-kafka2postgresql-flow",
            tag="0.1",
            dockerfile="Dockerfile",
            platform="linux/arm64",
            buildargs={
                       "TOPIC_NAME": os.getenv("TOPIC_NAME"),
                       "KAFKA_URL": os.getenv("KAFKA_URL"),
                       "DB_URL": os.getenv("DB_URL"),
                       },
        ),
        schedule=(CronSchedule(cron="*/5 8-20 * * *", timezone="Asia/Seoul")),
        build=True,
    )