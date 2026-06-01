FROM astrocrpublic.azurecr.io/runtime:3.1-5

USER root

# Copy the observe library source into the image. The build context is the
# repo root (set in docker-compose.yml), so paths are relative to that.
COPY --chown=astro:astro observe /usr/local/airflow/observe_lib/observe
COPY --chown=astro:astro pyproject.toml /usr/local/airflow/observe_lib/
COPY --chown=astro:astro README.md /usr/local/airflow/observe_lib/

# Install the observe library from source baked into the image. The source is
# COPY'd in above (not volume-mounted), so a normal non-editable install is used
# here — matching the wheel-based install in the sibling projects' Dockerfiles.
RUN pip install --no-cache-dir /usr/local/airflow/observe_lib/

USER astro
