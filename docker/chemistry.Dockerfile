FROM mambaorg/micromamba:2.3.3 AS environment
COPY --chown=$MAMBA_USER:$MAMBA_USER chemistry/environment.yml /tmp/environment.yml
RUN micromamba create --yes --name chemistry --file /tmp/environment.yml && micromamba clean --all --yes

FROM environment
ARG MAMBA_DOCKERFILE_ACTIVATE=1
USER root
RUN useradd --uid 65532 --no-create-home --shell /usr/sbin/nologin chemistry
COPY --chown=65532:65532 chemistry_runner /opt/library-prep/chemistry_runner
COPY --chown=65532:65532 library_pipeline.py run_conformers_chunked.py /opt/library-prep/
ENV PYTHONPATH=/opt/library-prep PYTHONUNBUFFERED=1 HOME=/tmp
WORKDIR /opt/library-prep
USER 65532:65532
ENTRYPOINT ["micromamba", "run", "--no-capture-output", "-n", "chemistry", "python", "-m", "chemistry_runner"]
