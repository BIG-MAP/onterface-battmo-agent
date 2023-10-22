FROM  ubuntu:22.04
#FROM jupyter/datascience-notebook:ubuntu-20.04
#FROM jupyter/datascience-notebook
#FROM python:3.9

#USER root
RUN apt-get --yes update  && \
    #apt-get --yes install software-properties-common && \
    #apt-add-repository ppa:octave/stable && \
    #add-apt-repository ppa:deadsnakes/ppa && \
    #apt-get --yes update && \
    DEBIAN_FRONTEND="noninteractive" \
    apt-get --no-install-recommends --yes install \
      git \
      octave \
      build-essential \
      liboctave-dev \
      gnuplot \
      fonts-freefont-otf \
      tzdata \
      #python3.9 \
      python3-pip

# octave 6.4.0 has no json support
#RUN octave --eval "pkg install https://github.com/apjanke/octave-jsonstuff/releases/download/v0.3.3/jsonstuff-0.3.3.tar.gz"
#RUN octave --eval "pkg load jsonstuff"

# Switch back to jovyan to avoid accidental container runs as root
#USER ${NB_UID}

#RUN conda install xeus-octave -c conda-forge
RUN pip install -U pip # https://github.com/pypa/pip/issues/10851
RUN pip install 'pydantic<2.0' oct2py "prefect==2.13.8" "osw==0.14.2" zenodo_client 
#big-map-archive-api-client
#RUN pip install "big-map-archive-api-client@git+https://github.com/materialscloud-org/big-map-archive-api-client@0b4672f849f204535dfe74aa4e39923bb6b1c166"
#RUN pip install "sdlabs_wrapper@git+https://github.com/Atinary-technologies/sdlabs_wrapper@v0.5.0"

