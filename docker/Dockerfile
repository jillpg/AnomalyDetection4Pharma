# ---------------------------
# Etapa 1: Base con Python 3.11 + Java + CUDA
# ---------------------------
FROM nvidia/cuda:12.6.0-cudnn-devel-ubuntu22.04 AS base

# Instalar Python 3.11, Java y dependencias
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-dev \
    python3-pip \
    build-essential \
    openjdk-17-jdk \
    wget \
    libgomp1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configurar Python 3.11 como predeterminado
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Variables de entorno Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin


# Configurar argumentos de PySpark para incluir paquetes de AWS automáticamente
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell"

# ---------------------------
# Etapa 2: Instalar Python Requirements y PySpark
# ---------------------------
FROM base AS python-env

COPY requirements.txt /tmp/requirements.txt

# Instalar pip y dependencias Python
RUN python -m pip install --upgrade pip && \
    python -m pip install -r /tmp/requirements.txt

# Descargar JARs para S3A
RUN mkdir -p /usr/local/lib/python3.11/site-packages/pyspark/jars/ && \
    wget -q -P /usr/local/lib/python3.11/site-packages/pyspark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q -P /usr/local/lib/python3.11/site-packages/pyspark/jars/ \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ---------------------------
# Etapa 3: Imagen final
# ---------------------------
FROM python-env AS final

WORKDIR /app

# Exponer puerto Jupyter
EXPOSE 8888

# Copiar código de la aplicación
COPY . .

# Comando por defecto para iniciar Jupyter Notebook
CMD ["jupyter", "notebook", "--allow-root", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
