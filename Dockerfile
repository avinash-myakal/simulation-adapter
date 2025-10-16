# =========================
# file: Dockerfile
# =========================
# Base: EnergyPlus 9.4.0 (Option A)
FROM nrel/energyplus:9.4.0

# OS deps often needed by wheels and EnergyPlus
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates libgomp1 libx11-6 && \
    rm -rf /var/lib/apt/lists/*

# ---- Install Python 3.11 via Miniforge (conda-forge) ----
# (Avoids Anaconda "defaults" TOS gate that breaks non-interactive builds)
ENV CONDA_DIR=/opt/conda
RUN curl -fsSL https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh -o /tmp/miniforge.sh && \
    bash /tmp/miniforge.sh -b -p $CONDA_DIR && \
    $CONDA_DIR/bin/conda config --set channel_priority strict && \
    $CONDA_DIR/bin/conda config --remove channels defaults || true && \
    $CONDA_DIR/bin/conda config --add channels conda-forge && \
    $CONDA_DIR/bin/conda install -y python=3.11 && \
    $CONDA_DIR/bin/conda clean -afy
ENV PATH="$CONDA_DIR/bin:${PATH}"

# ---- App setup ----
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY mm_final_energy_sim.py /app/mm_final_energy_sim.py
COPY simulation_adapter.py  /app/simulation_adapter.py

# ---- Runtime defaults (override with .env.docker / compose) ----
ENV ENERGYPLUS_EXE=energyplus \
    S3_ENDPOINT=http://minio:9000 \
    S3_BUCKET=mmstore \
    S3_IDF_PREFIX=output_idf_files/ \
    S3_EPW_PREFIX=weather/epw/ \
    RUN_TIMEOUT_SEC=3600 \
    ADAPTER_PORT=8000 \
    PYTHONUNBUFFERED=1

EXPOSE 8000

# Gunicorn captures stdout/stderr so print() shows in docker logs
CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:8000", "--timeout", "0", "--capture-output", "--access-logfile", "-", "--log-level", "info", "simulation_adapter:app"]

