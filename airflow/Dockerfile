FROM astrocrpublic.azurecr.io/runtime:3.1-5

USER root

# Copy the observe library source into the image. The build context is the
# repo root (set in docker-compose.yml), so paths are relative to that.
COPY --chown=astro:astro observe /usr/local/airflow/observe_lib/observe
COPY --chown=astro:astro pyproject.toml /usr/local/airflow/observe_lib/
COPY --chown=astro:astro README.md /usr/local/airflow/observe_lib/

# Install in editable mode so changes to observe/ are reflected immediately
RUN pip install --no-cache-dir -e /usr/local/airflow/observe_lib/

USER astro
