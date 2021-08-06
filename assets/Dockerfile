# Build on top of Ubuntu LTS (focal)
FROM ubuntu:20.04

# Set Conda environment name
ARG ENV_NAME=agha

# Install some packages through system package manager
RUN \
  apt-get update && \
  apt-get install --yes --no-install-recommends \
    ca-certificates  \
    curl \
    unzip && \
  # Clean up
  apt-get clean && \
  rm -rf /var/lib/apt/*

# Install Miniconda
RUN \
  curl 'https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh' -o 'miniconda.sh' && \
  /bin/bash miniconda.sh -b -p /opt/miniconda/ && \
  rm miniconda.sh

# Prepare Conda environment
# Execute RUN with /bin/bash; /bin/sh produces error when deactivating Conda env
SHELL ["/bin/bash", "-c"]
RUN \
  # Source Miniconda software
  . /opt/miniconda/etc/profile.d/conda.sh && \
  # Create and activate new environment, install mamba
  conda create -n "${ENV_NAME}" && \
  conda activate "${ENV_NAME}" && \
  conda install -c conda-forge mamba && \
  # Install dependencies, clean up
  mamba install \
    # Provides working release of fqtools. See assets/conda/fqtools for build process
    --channel scwatts \
    --channel bioconda \
    --channel conda-forge \
    --channel defaults \
    --yes \
    bcftools \
    boto3 \
    fqtools \
    samtools \
    && \
  # Clean up
  # Base Conda directory, including /opt/miniconda/pkgs/
  conda clean --all --force-pkgs-dirs --yes && \
  # New Conda env, including /opt/miniconda/envs/${ENV_NAME}/pkgs/
  /opt/miniconda/envs/${ENV_NAME}/bin/conda clean --all --force-pkgs-dirs --yes
# Revert RUN execution shell to /bin/sh as good practise
SHELL ["/bin/sh", "-c"]

# Add Conda env to PATH
ENV PATH="/opt/miniconda/envs/${ENV_NAME}/bin:/opt/miniconda/bin:${PATH}"
ENV CONDA_PREFIX="/opt/miniconda/envs/${ENV_NAME}"

# Copy in 'shared' Python package and execution script
COPY assets/validate_file.py /opt/
COPY lambdas/layers/shared /opt/shared

# Install 'shared' Python package
RUN \
    pip install /opt/shared/ && \
    rm -r /opt/shared/
