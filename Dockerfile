FROM ghcr.io/kbase/cdm-spark-standalone:pr-42

# This is a container specifically for running tests. It is not intended to be deployed anywhere.

USER root

RUN mkdir /uvinstall

WORKDIR /uvinstall

COPY pyproject.toml uv.lock .python-version .

ENV UV_PROJECT_ENVIRONMENT=/opt/bitnami/python
RUN uv sync --locked --inexact --dev

RUN mkdir /imp_test && chown spark_user /imp_test

COPY entrypoint.sh /

ENV PYTHONPATH=/imp_test:/imp_test/test

WORKDIR /imp_test

USER spark_user

ENV IMP_SPARK_JARS_DIR=/opt/bitnami/spark/jars

ENTRYPOINT ["tini", "--", "/entrypoint.sh"]
