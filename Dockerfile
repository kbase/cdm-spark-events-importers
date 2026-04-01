FROM ghcr.io/berdatalakehouse/kube_spark_manager_image:pr-16

# This is a container specifically for running tests. It is not intended to be deployed anywhere.

USER root

RUN mkdir /uvinstall

WORKDIR /uvinstall

COPY pyproject.toml uv.lock .python-version .

ENV UV_PROJECT_ENVIRONMENT=/opt/conda
RUN uv sync --locked --inexact --dev

RUN mkdir /imp_test

COPY entrypoint.sh /

ENV PYTHONPATH=/imp_test:/imp_test/test

WORKDIR /imp_test

#USER spark_user  the kube image doesn't have this user'

ENV IMP_SPARK_JARS_DIR=/usr/local/spark/jars

ENTRYPOINT ["tini", "--", "/entrypoint.sh"]
