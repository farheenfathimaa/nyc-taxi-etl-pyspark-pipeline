# Airflow image with a JVM + PySpark bundled, so the pipeline's Spark jobs can run
# in local[*] mode straight from an Airflow task (BashOperator/PythonOperator).
# Engineered for local development on a laptop - no cluster, no paid services.
FROM apache/airflow:2.10.5-python3.11

# --- root: OS-level deps -----------------------------------------------------
USER root

# curl: webserver healthchecks. openjdk-17-jre-headless: PySpark needs a JVM.
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        curl \
        openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Resolve the JRE path for the running architecture (amd64 | arm64).
# NOTE: Docker ENV has no command substitution, so we resolve it inside a RUN and
# write it to /etc/profile.d (helpful for `docker compose exec sh -lc`). The Python
# ETL also resolves java from PATH as a fallback, so no JAVA_HOME is strictly needed.
RUN dpkgArch="$(dpkg --print-architecture)" \
    && echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-$dpkgArch" >> /etc/profile.d/java_env.sh \
    && echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/profile.d/java_env.sh

# --- airflow user: Python deps ----------------------------------------------
USER airflow

# Kept in its own file so the same deps can be reused on a dedicated Spark box.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt