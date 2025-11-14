FROM jupyter/pyspark-notebook:latest

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    vim \
    && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Install Python packages
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Configure Jupyter
RUN jupyter lab --generate-config && \
    echo "c.NotebookApp.token = ''" >> ~/.jupyter/jupyter_lab_config.py && \
    echo "c.NotebookApp.password = ''" >> ~/.jupyter/jupyter_lab_config.py && \
    echo "c.NotebookApp.allow_origin = '*'" >> ~/.jupyter/jupyter_lab_config.py && \
    echo "c.NotebookApp.disable_check_xsrf = True" >> ~/.jupyter/jupyter_lab_config.py

# Set working directory
WORKDIR /home/jovyan/work

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
