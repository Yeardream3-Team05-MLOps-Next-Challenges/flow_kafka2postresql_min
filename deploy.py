import os

from prefect import flow
from prefect.deployments import DeploymentImage
from prefect.client.schemas.schedules import CronSchedule

from flow import hun_min_kafka2postgresql_flow

if __name__ == "__main__":
    hun_min_kafka2postgresql_flow.deploy(
        name="hun_min_kafka2postgresql_deploy",
        work_pool_name="docker-agent-pool",
        work_queue_name="docker-agent",
        image=DeploymentImage(
            name="hun-min-kafka2postgresql",
            tag=os.getenv("VERSION"),
            dockerfile="Dockerfile",
            platform="linux/arm64",
            buildargs={
                       "LOGGING_LEVEL": os.getenv("LOGGING_LEVEL"),
                       "TOPIC_NAME": os.getenv("TOPIC_NAME"),
                       "KAFKA_URL": os.getenv("KAFKA_URL"),
                       "DB_URL": os.getenv("DB_URL"),
                       },
        ),
        schedule=(CronSchedule(cron="*/5 8-20 * * 1-5", timezone="Asia/Seoul")),
        build=True,
    )